"""
Zoomex Private WebSocket Client

Provides real-time order, position, and wallet updates from Zoomex exchange.
Requires API key authentication.
"""
import json
import time
import hmac
import hashlib
import threading
import os
from typing import Dict, Any, Optional, Callable, List
import websocket as ws
from urllib.parse import urlparse

from logger import setup_logger

logger = setup_logger("zoomex_private_ws")

# Zoomex Private WebSocket endpoints
ZOOMEX_WS_PRIVATE_URL = "wss://stream.zoomex.com/v3/private"
ZOOMEX_WS_PRIVATE_TESTNET_URL = "wss://stream-testnet.zoomex.com/v3/private"


class ZoomexPrivateWebSocket:
    """Private WebSocket client for Zoomex exchange.
    
    Provides real-time updates for:
    - Order status changes
    - Position updates
    - Wallet/balance changes
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        on_message_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_order_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_position_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_wallet_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        auto_reconnect: bool = True,
        proxy: Optional[str] = None,
        testnet: bool = False,
    ):
        """
        Initialize Zoomex private WebSocket client.

        Args:
            api_key: Zoomex API key
            api_secret: Zoomex API secret
            on_message_callback: General callback for all messages
            on_order_callback: Callback for order updates
            on_position_callback: Callback for position updates
            on_wallet_callback: Callback for wallet updates
            auto_reconnect: Whether to automatically reconnect on disconnect
            proxy: Proxy URL (e.g., http://host:port)
            testnet: Use testnet endpoint
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_message_callback = on_message_callback
        self.on_order_callback = on_order_callback
        self.on_position_callback = on_position_callback
        self.on_wallet_callback = on_wallet_callback
        self.auto_reconnect = auto_reconnect
        self.testnet = testnet

        # Connection state
        self.ws: Optional[ws.WebSocketApp] = None
        self.connected = False
        self.authenticated = False
        self.running = False

        # Order/Position state tracking
        self.open_orders: Dict[str, Dict] = {}  # orderId -> order data
        self.positions: Dict[str, Dict] = {}    # symbol -> position data
        self.balances: Dict[str, Dict] = {}     # coin -> balance data

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
        self.heartbeat_interval = 20

        # Proxy
        if proxy is None:
            proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
        self.proxy = proxy

    def _get_ws_url(self) -> str:
        """Get WebSocket URL."""
        if self.testnet:
            return ZOOMEX_WS_PRIVATE_TESTNET_URL
        return ZOOMEX_WS_PRIVATE_URL

    def _generate_auth_signature(self, expires: int) -> str:
        """Generate authentication signature.
        
        Signature format: HMAC_SHA256("GET/realtime" + expires)
        """
        message = f"GET/realtime{expires}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

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
        logger.info(f"Connecting to Zoomex Private WebSocket: {url}")

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
        logger.info("Zoomex Private WebSocket connected")
        self.connected = True
        self.authenticated = False
        self.reconnect_attempts = 0
        self.reconnecting = False
        self.last_heartbeat = time.time()

        # Authenticate
        self._authenticate()

    def _authenticate(self):
        """Send authentication message."""
        expires = int((time.time() + 10) * 1000)  # 10 seconds from now
        signature = self._generate_auth_signature(expires)
        
        auth_msg = {
            "op": "auth",
            "args": [self.api_key, str(expires), signature]
        }
        
        try:
            self.ws.send(json.dumps(auth_msg))
            logger.info("Sent authentication request")
        except Exception as e:
            logger.error(f"Authentication send failed: {e}")

    def _on_message(self, ws_app, message: str):
        """Handle incoming message."""
        try:
            data = json.loads(message)
            self.last_heartbeat = time.time()

            # Handle pong
            if data.get("op") == "pong":
                logger.debug("Received pong")
                return

            # Handle auth response
            if data.get("op") == "auth":
                if data.get("success"):
                    logger.info("Authentication successful")
                    self.authenticated = True
                    # Subscribe to private streams after auth
                    self._subscribe_private_streams()
                else:
                    logger.error(f"Authentication failed: {data.get('ret_msg')}")
                return

            # Handle subscription response
            if data.get("op") == "subscribe":
                if data.get("success"):
                    logger.info(f"Subscribed to private stream")
                else:
                    logger.error(f"Subscription failed: {data.get('ret_msg')}")
                return

            # Process private data
            topic = data.get("topic", "")
            payload_list = data.get("data", [])
            
            if not payload_list:
                return

            if topic == "order":
                self._process_orders(payload_list)
            elif topic == "position":
                self._process_positions(payload_list)
            elif topic == "wallet":
                self._process_wallet(payload_list)

            if self.on_message_callback and topic:
                self.on_message_callback(topic, {"data": payload_list})

        except Exception as e:
            logger.error(f"Message error: {e}")

    def _subscribe_private_streams(self):
        """Subscribe to private data streams."""
        topics = ["order", "position", "wallet"]
        msg = json.dumps({"op": "subscribe", "args": topics})
        try:
            self.ws.send(msg)
            logger.info(f"Subscribing to private streams: {topics}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

    def _process_orders(self, orders: List[Dict]):
        """Process order updates."""
        for order in orders:
            order_id = order.get("orderId")
            status = order.get("orderStatus")
            
            if not order_id:
                continue

            # Parse order data
            parsed_order = {
                "orderId": order_id,
                "orderLinkId": order.get("orderLinkId"),
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "orderType": order.get("orderType"),
                "price": order.get("price"),
                "qty": order.get("qty"),
                "filledQty": order.get("cumExecQty"),
                "avgPrice": order.get("avgPrice"),
                "status": status,
                "timeInForce": order.get("timeInForce"),
                "reduceOnly": order.get("reduceOnly"),
                "createdTime": order.get("createdTime"),
                "updatedTime": order.get("updatedTime"),
                "raw": order
            }

            # Update order tracking
            if status in ["New", "PartiallyFilled", "Untriggered"]:
                self.open_orders[order_id] = parsed_order
            else:
                # Order is filled, cancelled, or rejected
                self.open_orders.pop(order_id, None)

            logger.info(f"Order update: {order.get('symbol')} {order.get('side')} "
                       f"{order.get('qty')} @ {order.get('price')} - {status}")

            if self.on_order_callback:
                self.on_order_callback(parsed_order)

    def _process_positions(self, positions: List[Dict]):
        """Process position updates."""
        for pos in positions:
            symbol = pos.get("symbol")
            if not symbol:
                continue

            parsed_position = {
                "symbol": symbol,
                "side": pos.get("side"),
                "size": float(pos.get("size", 0)),
                "entryPrice": pos.get("avgPrice") or pos.get("entryPrice"),
                "markPrice": pos.get("markPrice"),
                "unrealizedPnl": pos.get("unrealisedPnl"),
                "realizedPnl": pos.get("cumRealisedPnl"),
                "leverage": pos.get("leverage"),
                "liquidationPrice": pos.get("liqPrice"),
                "positionValue": pos.get("positionValue"),
                "positionIdx": pos.get("positionIdx"),
                "raw": pos
            }

            # Update position tracking
            if parsed_position["size"] > 0:
                self.positions[symbol] = parsed_position
            else:
                self.positions.pop(symbol, None)

            logger.info(f"Position update: {symbol} {pos.get('side')} "
                       f"size={pos.get('size')} PnL={pos.get('unrealisedPnl')}")

            if self.on_position_callback:
                self.on_position_callback(parsed_position)

    def _process_wallet(self, wallet_data: List[Dict]):
        """Process wallet/balance updates."""
        for account in wallet_data:
            coins = account.get("coin", [])
            for coin in coins:
                coin_name = coin.get("coin")
                if not coin_name:
                    continue

                parsed_balance = {
                    "coin": coin_name,
                    "available": coin.get("availableToWithdraw"),
                    "walletBalance": coin.get("walletBalance"),
                    "equity": coin.get("equity"),
                    "unrealizedPnl": coin.get("unrealisedPnl"),
                    "raw": coin
                }

                self.balances[coin_name] = parsed_balance

                logger.debug(f"Wallet update: {coin_name} balance={coin.get('walletBalance')}")

                if self.on_wallet_callback:
                    self.on_wallet_callback(parsed_balance)

    def _on_error(self, ws_app, error):
        """Handle error."""
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws_app, close_status_code, close_msg):
        """Handle connection closed."""
        logger.warning(f"WebSocket closed: {close_status_code}")
        self.connected = False
        self.authenticated = False
        if self.running and self.auto_reconnect and not self.reconnecting:
            threading.Thread(target=self.reconnect, daemon=True).start()

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
            self.authenticated = False
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
        logger.info("Disconnecting Zoomex Private WebSocket...")
        self.running = False
        self.connected = False
        self.authenticated = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
        logger.info("Disconnected")

    def is_connected(self) -> bool:
        """Check connection status."""
        return self.connected and self.authenticated

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """Get tracked open orders."""
        if symbol:
            return [o for o in self.open_orders.values() if o.get("symbol") == symbol]
        return list(self.open_orders.values())

    def get_position(self, symbol: str) -> Optional[Dict]:
        """Get tracked position for a symbol."""
        return self.positions.get(symbol)

    def get_all_positions(self) -> List[Dict]:
        """Get all tracked positions."""
        return list(self.positions.values())

    def get_balance(self, coin: str = "USDT") -> Optional[Dict]:
        """Get tracked balance for a coin."""
        return self.balances.get(coin)
