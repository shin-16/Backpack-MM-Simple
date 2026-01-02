"""
Zoomex Public WebSocket Client

Provides real-time price data and order book updates from Zoomex exchange.
"""
import json
import time
import threading
import os
from typing import Dict, Any, Optional, Callable, List, Tuple
import websocket as ws
from urllib.parse import urlparse

from logger import setup_logger

logger = setup_logger("zoomex_ws")

# Zoomex WebSocket endpoints
ZOOMEX_WS_PUBLIC_URL = "wss://stream.zoomex.com/v5/public/linear"
ZOOMEX_WS_TESTNET_URL = "wss://stream-testnet.zoomex.com/v5/public/linear"


class ZoomexWebSocket:
    """Public WebSocket client for Zoomex exchange."""

    def __init__(
        self,
        symbol: str,
        on_message_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        auto_reconnect: bool = True,
        proxy: Optional[str] = None,
        testnet: bool = False,
    ):
        """
        Initialize Zoomex public WebSocket client.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            on_message_callback: Callback function for incoming messages
            auto_reconnect: Whether to automatically reconnect on disconnect
            proxy: Proxy URL (e.g., http://host:port)
            testnet: Use testnet endpoint
        """
        # Normalize symbol to BTCUSDT format
        self.symbol = symbol.upper().replace("-", "").replace("_", "")
        self.on_message_callback = on_message_callback
        self.auto_reconnect = auto_reconnect
        self.testnet = testnet

        # Connection state
        self.ws: Optional[ws.WebSocketApp] = None
        self.connected = False
        self.running = False
        self.subscriptions: List[str] = []  # Track active subscriptions

        # Price data
        self.last_price: Optional[float] = None
        self.bid_price: Optional[float] = None
        self.ask_price: Optional[float] = None
        self.index_price: Optional[float] = None
        self.mark_price: Optional[float] = None
        self.funding_rate: Optional[float] = None
        self.orderbook: Dict[str, List] = {"bids": [], "asks": []}
        self.historical_prices: List[float] = []
        self.max_price_history = 100

        # Depth Update ID tracking for packet loss detection
        self.last_update_id: Optional[int] = None
        self.depth_snapshot_received = False
        self.packet_loss_count = 0

        # Reconnection
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnecting = False

        # Threading
        self.ws_thread: Optional[threading.Thread] = None
        self.ws_lock = threading.Lock()
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 20  # Zoomex requires ping every 20s

        # Proxy
        if proxy is None:
            proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
        self.proxy = proxy

    def _get_ws_url(self) -> str:
        """Get WebSocket URL."""
        if self.testnet:
            return ZOOMEX_WS_TESTNET_URL
        return ZOOMEX_WS_PUBLIC_URL

    def _parse_proxy(self) -> tuple:
        """Parse proxy URL into components."""
        if not self.proxy:
            return None, None, None, None
        parsed = urlparse(self.proxy)
        auth = None
        if parsed.username and parsed.password:
            auth = (parsed.username, parsed.password)
        proxy_type = parsed.scheme if parsed.scheme in ["http", "socks4", "socks5"] else "http"
        return parsed.hostname, parsed.port, auth, proxy_type

    def connect(self):
        """Establish WebSocket connection."""
        self.running = True
        self.reconnect_attempts = 0
        self.reconnecting = False

        ws.enableTrace(False)
        url = self._get_ws_url()
        logger.info(f"Connecting to Zoomex WebSocket: {url}")

        self.ws = ws.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.ws_thread = threading.Thread(target=self._ws_run_forever, daemon=True)
        self.ws_thread.start()
        self._start_heartbeat()

    def initialize_orderbook(self) -> bool:
        """Initialize orderbook - stub for strategy compatibility.
        
        Zoomex automatically receives orderbook snapshot when subscribing.
        Returns True to indicate success.
        """
        return True

    def _ws_run_forever(self):
        """Run WebSocket event loop."""
        try:
            host, port, auth, proxy_type = self._parse_proxy()
            self.ws.run_forever(
                ping_interval=0,
                ping_timeout=10,
                http_proxy_host=host,
                http_proxy_port=port,
                http_proxy_auth=auth,
                proxy_type=proxy_type,
            )
        except Exception as e:
            logger.error(f"WebSocket error: {e}")

    def _start_heartbeat(self):
        """Start heartbeat thread."""
        if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.heartbeat_thread.start()

    def _heartbeat_loop(self):
        """Send periodic ping messages."""
        while self.running:
            try:
                now = time.time()
                if now - self.last_heartbeat >= self.heartbeat_interval:
                    if self.connected and self.ws:
                        ping_msg = json.dumps({"op": "ping"})
                        self.ws.send(ping_msg)
                        logger.debug("Sent ping")
                        self.last_heartbeat = now

                if now - self.last_heartbeat > self.heartbeat_interval * 3:
                    if self.auto_reconnect and not self.reconnecting:
                        logger.warning("Heartbeat timeout, reconnecting...")
                        threading.Thread(target=self.reconnect, daemon=True).start()

                time.sleep(5)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                time.sleep(5)

    def _on_open(self, ws_app):
        """Handle connection opened."""
        logger.info("Zoomex WebSocket connected")
        self.connected = True
        self.reconnect_attempts = 0
        self.reconnecting = False
        self.last_heartbeat = time.time()

        time.sleep(0.3)
        self.subscribe_ticker()
        self.subscribe_depth()
        
        # Reset depth tracking on new connection
        self.last_update_id = None
        self.depth_snapshot_received = False

    def _on_message(self, ws_app, message: str):
        """Handle incoming message."""
        try:
            data = json.loads(message)
            self.last_heartbeat = time.time()

            # Handle pong
            if data.get("op") == "pong":
                logger.debug("Received pong")
                return

            # Handle subscription response
            if data.get("success") is not None:
                if data.get("success"):
                    logger.info(f"Subscribed: {data.get('req_id', 'unknown')}")
                else:
                    logger.error(f"Subscription failed: {data.get('ret_msg')}")
                return

            # Process data
            topic = data.get("topic", "")
            msg_type = data.get("type", "")  # snapshot or delta
            payload = data.get("data", {})

            if topic.startswith("tickers."):
                self._process_ticker(payload)
            elif topic.startswith("orderbook."):
                self._process_depth(payload, msg_type)
            elif topic.startswith("publicTrade."):
                self._process_trades(payload)

            if self.on_message_callback and topic:
                self.on_message_callback(topic, payload)

        except Exception as e:
            logger.error(f"Message error: {e}")

    def _process_ticker(self, data: Dict[str, Any]):
        """Process ticker data."""
        if not isinstance(data, dict):
            return
            
        if "lastPrice" in data:
            try:
                self.last_price = float(data["lastPrice"])
                self.historical_prices.append(self.last_price)
                if len(self.historical_prices) > self.max_price_history:
                    self.historical_prices = self.historical_prices[-self.max_price_history:]
            except (ValueError, TypeError):
                pass
                
        if "indexPrice" in data:
            try:
                self.index_price = float(data["indexPrice"])
            except (ValueError, TypeError):
                pass
                
        if "markPrice" in data:
            try:
                self.mark_price = float(data["markPrice"])
            except (ValueError, TypeError):
                pass
                
        if "fundingRate" in data:
            try:
                self.funding_rate = float(data["fundingRate"])
            except (ValueError, TypeError):
                pass

        if "bid1Price" in data:
            try:
                self.bid_price = float(data["bid1Price"])
            except (ValueError, TypeError):
                pass

        if "ask1Price" in data:
            try:
                self.ask_price = float(data["ask1Price"])
            except (ValueError, TypeError):
                pass

    def _process_depth(self, data: Dict[str, Any], msg_type: str):
        """Process orderbook depth with Update ID tracking.
        
        Args:
            data: Orderbook data with bids (b) and asks (a)
            msg_type: 'snapshot' or 'delta'
        """
        if not isinstance(data, dict):
            return

        bids = data.get("b", [])
        asks = data.get("a", [])
        update_id = data.get("u")

        if msg_type == "snapshot":
            # Snapshot resets the orderbook and Update ID
            self.orderbook = {"bids": bids, "asks": asks}
            self.last_update_id = update_id if update_id else 1
            self.depth_snapshot_received = True
            logger.debug(f"Depth snapshot received, Update ID: {self.last_update_id}")
        else:
            # Delta update - check for packet loss
            if update_id is not None and self.last_update_id is not None:
                expected_id = self.last_update_id + 1
                if update_id != expected_id:
                    self.packet_loss_count += 1
                    logger.warning(
                        f"Packet loss detected! Expected {expected_id}, got {update_id}. "
                        f"Total losses: {self.packet_loss_count}"
                    )
                    if self.packet_loss_count % 5 == 0:
                        logger.info("Resubscribing to depth for fresh snapshot...")
                        self.depth_snapshot_received = False
                        threading.Thread(target=self._resubscribe_depth, daemon=True).start()
                        return
            
            if update_id is not None:
                self.last_update_id = update_id
            
            # Apply delta update
            self._update_orderbook_side("bids", bids)
            self._update_orderbook_side("asks", asks)

        # Update best bid/ask
        self._sort_orderbook()

    def _sort_orderbook(self):
        """Sort orderbook and update best bid/ask."""
        if self.orderbook["bids"]:
            try:
                sorted_bids = sorted(self.orderbook["bids"], key=lambda x: float(x[0]), reverse=True)
                self.orderbook["bids"] = sorted_bids
                self.bid_price = float(sorted_bids[0][0])
            except (ValueError, TypeError, IndexError):
                pass

        if self.orderbook["asks"]:
            try:
                sorted_asks = sorted(self.orderbook["asks"], key=lambda x: float(x[0]))
                self.orderbook["asks"] = sorted_asks
                self.ask_price = float(sorted_asks[0][0])
            except (ValueError, TypeError, IndexError):
                pass

    def _resubscribe_depth(self):
        """Resubscribe to depth to get a fresh snapshot."""
        time.sleep(0.5)
        self.subscribe_depth()

    def _update_orderbook_side(self, side: str, updates: List):
        """Apply delta updates to orderbook."""
        current = {item[0]: item for item in self.orderbook.get(side, [])}
        for update in updates:
            price, qty = update[0], update[1]
            if float(qty) == 0:
                current.pop(price, None)
            else:
                current[price] = update
        self.orderbook[side] = list(current.values())

    def _process_trades(self, data):
        """Process trades data."""
        if isinstance(data, list):
            for trade in data:
                if isinstance(trade, dict) and "p" in trade:
                    try:
                        self.last_price = float(trade["p"])
                    except (ValueError, TypeError):
                        pass

    def _on_error(self, ws_app, error):
        """Handle error."""
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws_app, close_status_code, close_msg):
        """Handle connection closed."""
        logger.warning(f"WebSocket closed: {close_status_code}")
        self.connected = False
        if self.running and self.auto_reconnect and not self.reconnecting:
            threading.Thread(target=self.reconnect, daemon=True).start()

    def subscribe_ticker(self):
        """Subscribe to ticker stream."""
        if not self.connected or not self.ws:
            return False
        topic = f"tickers.{self.symbol}"
        return self._subscribe([topic])

    def subscribe_depth(self, depth: int = 50):
        """Subscribe to orderbook depth.
        
        Args:
            depth: Orderbook depth level (1, 50, 200, 500)
        """
        if not self.connected or not self.ws:
            return False
        topic = f"orderbook.{depth}.{self.symbol}"
        return self._subscribe([topic])

    def subscribe_trades(self):
        """Subscribe to trades stream."""
        if not self.connected or not self.ws:
            return False
        topic = f"publicTrade.{self.symbol}"
        return self._subscribe([topic])

    def subscribe_bookTicker(self):
        """Subscribe to book ticker (best bid/ask) - alias for ticker subscription.
        
        For strategy compatibility - Zoomex ticker includes bid1/ask1 prices.
        """
        return self.subscribe_ticker()

    def private_subscribe(self, stream: str) -> bool:
        """Private subscription stub for strategy compatibility.
        
        Zoomex public WebSocket doesn't support private streams.
        Use Zoomex private WebSocket client for private data.
        """
        logger.debug(f"Private subscription not supported in public client: {stream}")
        return False

    def _subscribe(self, topics: List[str]) -> bool:
        """Send subscription."""
        try:
            msg = json.dumps({"op": "subscribe", "args": topics})
            self.ws.send(msg)
            logger.info(f"Subscribing to: {topics}")
            return True
        except Exception as e:
            logger.error(f"Subscribe error: {e}")
            return False

    def reconnect(self) -> bool:
        """Reconnect to WebSocket."""
        if self.reconnecting:
            return False

        with self.ws_lock:
            if self.reconnect_attempts >= self.max_reconnect_attempts:
                logger.error("Max reconnect attempts reached")
                return False

            self.reconnecting = True
            self.reconnect_attempts += 1
            delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), self.max_reconnect_delay)

            logger.info(f"Reconnecting (attempt {self.reconnect_attempts}) in {delay}s...")
            time.sleep(delay)

            self.connected = False
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.ws = None

            try:
                url = self._get_ws_url()
                self.ws = ws.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws_thread = threading.Thread(target=self._ws_run_forever, daemon=True)
                self.ws_thread.start()
                self.last_heartbeat = time.time()
                self.reconnecting = False
                return True
            except Exception as e:
                logger.error(f"Reconnect failed: {e}")
                self.reconnecting = False
                return False

    def disconnect(self):
        """Disconnect WebSocket."""
        logger.info("Disconnecting Zoomex WebSocket...")
        self.running = False
        self.connected = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
        logger.info("Disconnected")

    def is_connected(self) -> bool:
        """Check connection status."""
        return self.connected

    def get_mid_price(self) -> Optional[float]:
        """Get mid price."""
        if self.bid_price and self.ask_price:
            return (self.bid_price + self.ask_price) / 2
        return self.last_price

    def get_current_price(self) -> Optional[float]:
        """Get current price (alias for last_price for strategy compatibility)."""
        return self.last_price

    def get_bid_ask(self) -> Tuple[Optional[float], Optional[float]]:
        """Get best bid and ask prices for strategy compatibility."""
        return self.bid_price, self.ask_price

    def check_and_reconnect_if_needed(self) -> bool:
        """Check connection and trigger reconnect if needed."""
        if not self.connected and not self.reconnecting:
            return self.reconnect()
        return self.connected

    def close(self):
        """Alias for disconnect() for strategy compatibility."""
        self.disconnect()


