"""
APEX Omni Private WebSocket Client

Provides authenticated access to account updates, order fills, and notifications.

Events supported (per APEX docs):
- Order Submission Push
- Post-Submission of Maker Order Push  
- Post-Cancel Order Push
- Successful Order Execution Push (fills)
- Deposit Success Push
- Withdrawal Request Submission Push
- Notify Message Push
"""
import json
import time
import threading
import os
import base64
import hashlib
import hmac
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List
import websocket as ws
from urllib.parse import urlparse

from logger import setup_logger

logger = setup_logger("apex_private_ws")

# APEX Omni Private WebSocket endpoint
APEX_WS_PRIVATE_URL = "wss://quote.omni.apex.exchange/realtime_private"


@dataclass
class Order:
    """Order data from APEX WebSocket."""
    id: str
    symbol: str
    side: str  # BUY/SELL
    type: str  # LIMIT, MARKET, STOP_MARKET, TAKE_PROFIT_MARKET
    status: str  # PENDING, OPEN, FILLED, CANCELED, UNTRIGGERED
    price: str
    size: str
    remaining_size: str
    cum_fill_size: str = "0"
    cum_fill_value: str = "0"
    cum_fill_fee: str = "0"
    client_id: str = ""
    time_in_force: str = ""
    reduce_only: bool = False
    trigger_price: str = ""
    created_at: int = 0
    updated_at: int = 0


@dataclass
class Fill:
    """Fill/trade data from APEX WebSocket."""
    id: str
    order_id: str
    symbol: str
    side: str  # BUY/SELL
    price: str
    size: str
    fee: str
    liquidity: str  # MAKER/TAKER
    quote_amount: str = "0"
    is_open: bool = True
    created_at: int = 0
    updated_at: int = 0


@dataclass 
class Position:
    """Position data from APEX WebSocket."""
    symbol: str
    side: str  # LONG/SHORT
    size: str
    entry_price: str
    exit_price: str = "0"
    realized_pnl: str = "0"
    funding_fee: str = "0"
    sum_open: str = "0"
    sum_close: str = "0"
    open_value: str = "0"
    custom_imr: str = "0"
    updated_at: int = 0


@dataclass
class Wallet:
    """Wallet/balance data from APEX WebSocket."""
    token: str  # USDT
    balance: str
    pending_deposit: str = "0"
    pending_withdraw: str = "0"
    pending_transfer_in: str = "0"
    pending_transfer_out: str = "0"


@dataclass
class Transfer:
    """Transfer (deposit/withdrawal) data from APEX WebSocket."""
    id: str
    type: str  # DEPOSIT/WITHDRAWAL
    status: str  # QUEUED, PENDING, CONFIRMED
    asset: str
    amount: str
    transaction_id: str = ""
    transaction_hash: str = ""
    created_at: int = 0
    confirmed_at: int = 0


@dataclass
class Notification:
    """Notification message from APEX WebSocket."""
    id: str
    category: str
    title: str
    content: str
    read: bool = False
    created_time: int = 0



class ApexPrivateWebSocket:
    """Private WebSocket client for APEX Omni exchange (authenticated)."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        passphrase: str,
        on_message_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        auto_reconnect: bool = True,
        proxy: Optional[str] = None,
    ):
        """
        Initialize APEX private WebSocket client.

        Args:
            api_key: APEX API key
            secret_key: APEX secret key
            passphrase: APEX passphrase
            on_message_callback: Callback function for incoming messages
            auto_reconnect: Whether to automatically reconnect on disconnect
            proxy: Proxy URL (e.g., http://host:port)
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase or ""
        self.on_message_callback = on_message_callback
        self.auto_reconnect = auto_reconnect

        # Event-specific callbacks
        self.on_order_callback: Optional[Callable[[Order], None]] = None
        self.on_fill_callback: Optional[Callable[[Fill], None]] = None
        self.on_position_callback: Optional[Callable[[Position], None]] = None
        self.on_wallet_callback: Optional[Callable[[Wallet], None]] = None
        self.on_transfer_callback: Optional[Callable[[Transfer], None]] = None
        self.on_notification_callback: Optional[Callable[[Notification], None]] = None

        # State tracking
        self.orders: Dict[str, Order] = {}  # order_id -> Order
        self.positions: Dict[str, Position] = {}  # symbol_side -> Position
        self.wallets: Dict[str, Wallet] = {}  # token -> Wallet
        self.fills: List[Fill] = []
        self.transfers: List[Transfer] = []
        self.notifications_list: List[Notification] = []
        
        # Legacy storage (for compatibility)
        self.order_updates: List[Dict[str, Any]] = []
        self.account_updates: List[Dict[str, Any]] = []
        self.notifications: List[Dict[str, Any]] = []

        # Connection state
        self.ws: Optional[ws.WebSocketApp] = None
        self.connected = False
        self.authenticated = False
        self.running = False

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
        self.heartbeat_interval = 15

        # Proxy
        if proxy is None:
            proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
        self.proxy = proxy

    def _get_ws_url(self) -> str:
        """Generate WebSocket URL with timestamp."""
        timestamp = int(time.time() * 1000)
        return f"{APEX_WS_PRIVATE_URL}?v=2&timestamp={timestamp}"

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

    def _generate_signature(self, timestamp: int) -> str:
        """
        Generate signature for authentication.
        
        Signature format: HMAC-SHA256(timestamp + 'GET' + '/ws/accounts')
        Key: base64_encode(secret_key)
        """
        request_path = "/ws/accounts"
        http_method = "GET"
        message = f"{timestamp}{http_method}{request_path}"

        # APEX uses base64 encoded secret for HMAC key
        key = base64.standard_b64encode(self.secret_key.encode('utf-8'))
        signature = hmac.new(key, message.encode('utf-8'), hashlib.sha256)
        return base64.standard_b64encode(signature.digest()).decode()

    def connect(self):
        """Establish private WebSocket connection."""
        if not self.api_key or not self.secret_key:
            logger.error("API key and secret key are required for private WebSocket")
            return

        self.running = True
        self.reconnect_attempts = 0
        self.reconnecting = False

        ws.enableTrace(False)
        url = self._get_ws_url()
        logger.info(f"Connecting to APEX private WebSocket...")

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
                        ts = str(int(now * 1000))
                        ping_msg = json.dumps({"op": "ping", "args": [ts]})
                        self.ws.send(ping_msg)
                        logger.debug("Sent ping")

                if now - self.last_heartbeat > self.heartbeat_interval * 3:
                    if self.auto_reconnect and not self.reconnecting:
                        logger.warning("Heartbeat timeout, reconnecting...")
                        threading.Thread(target=self.reconnect, daemon=True).start()

                time.sleep(5)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                time.sleep(5)

    def _on_open(self, ws_app):
        """Handle connection opened - send authentication."""
        logger.info("APEX private WebSocket connected, authenticating...")
        self.connected = True
        self.last_heartbeat = time.time()

        # Send authentication request
        time.sleep(0.3)
        self._send_auth()

    def _send_auth(self):
        """Send authentication request."""
        timestamp = int(time.time() * 1000)
        signature = self._generate_signature(timestamp)

        # Build auth request per APEX docs
        auth_request = {
            "type": "login",
            "topics": ["ws_zk_accounts_v3", "ws_notify_v1"],
            "httpMethod": "GET",
            "requestPath": "/ws/accounts",
            "apiKey": self.api_key,
            "passphrase": self.passphrase,
            "timestamp": timestamp,
            "signature": signature,
        }

        login_msg = json.dumps({
            "op": "login",
            "args": [json.dumps(auth_request)]
        })
        
        try:
            self.ws.send(login_msg)
            logger.info("Sent authentication request")
        except Exception as e:
            logger.error(f"Failed to send auth: {e}")

    def _on_message(self, ws_app, message: str):
        """Handle incoming message."""
        try:
            data = json.loads(message)
            self.last_heartbeat = time.time()

            # Handle pong
            if data.get("op") == "pong":
                return

            # Handle login/subscription response
            if "success" in data:
                if data.get("success"):
                    ret_msg = data.get("ret_msg", "")
                    if "login" in str(data.get("request", {})).lower() or not ret_msg:
                        logger.info("Authentication successful")
                        self.authenticated = True
                        self.reconnect_attempts = 0
                        self.reconnecting = False
                        # Subscribe to topics after auth
                        self._subscribe_topics()
                    else:
                        logger.info(f"Subscription successful: {data.get('request', {}).get('args', [])}")
                else:
                    error_msg = data.get("ret_msg", "unknown error")
                    logger.error(f"Request failed: {error_msg}")
                    if "authority" in error_msg.lower() or "login" in error_msg.lower():
                        self.authenticated = False
                return

            # Process data messages
            topic = data.get("topic", "")
            msg_type = data.get("type", "")
            payload = data.get("data", data.get("contents", {}))

            # Store updates based on topic
            if topic == "ws_zk_accounts_v3":
                self._process_account_update(payload)
            elif topic == "ws_notify_v1":
                self._process_notification(payload)

            # Call user callback
            if self.on_message_callback and topic:
                self.on_message_callback(topic, payload)

        except Exception as e:
            logger.error(f"Message error: {e}")

    def _subscribe_topics(self):
        """Subscribe to account and notification topics."""
        topics = ["ws_zk_accounts_v3", "ws_notify_v1"]
        try:
            msg = json.dumps({"op": "subscribe", "args": topics})
            self.ws.send(msg)
            logger.info(f"Subscribing to: {topics}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

    def _process_account_update(self, data: Any):
        """Process account/order update from ws_zk_accounts_v3 topic.
        
        Handles: orders, fills, positions, wallets (contractWallets/spotWallets), transfers
        """
        if isinstance(data, dict):
            self.account_updates.append(data)
            
            # Process orders
            orders_data = data.get("orders", [])
            for order_data in orders_data:
                order = self._parse_order(order_data)
                if order:
                    self.orders[order.id] = order
                    self.order_updates.append(order_data)
                    logger.info(f"Order: {order.symbol} {order.side} {order.size} @ {order.price} [{order.status}]")
                    if self.on_order_callback:
                        self.on_order_callback(order)
            
            # Process fills
            fills_data = data.get("fills", [])
            for fill_data in fills_data:
                fill = self._parse_fill(fill_data)
                if fill:
                    self.fills.append(fill)
                    if len(self.fills) > 100:
                        self.fills = self.fills[-100:]
                    logger.info(f"Fill: {fill.symbol} {fill.side} {fill.size} @ {fill.price} ({fill.liquidity})")
                    if self.on_fill_callback:
                        self.on_fill_callback(fill)
            
            # Process positions
            positions_data = data.get("positions", [])
            for pos_data in positions_data:
                pos = self._parse_position(pos_data)
                if pos:
                    key = f"{pos.symbol}_{pos.side}"
                    self.positions[key] = pos
                    logger.debug(f"Position: {pos.symbol} {pos.side} {pos.size} @ {pos.entry_price}")
                    if self.on_position_callback:
                        self.on_position_callback(pos)
            
            # Process contract wallets
            wallets_data = data.get("contractWallets", []) + data.get("spotWallets", []) + data.get("wallets", [])
            for wallet_data in wallets_data:
                wallet = self._parse_wallet(wallet_data)
                if wallet:
                    self.wallets[wallet.token] = wallet
                    logger.info(f"Wallet: {wallet.token} = {wallet.balance}")
                    if self.on_wallet_callback:
                        self.on_wallet_callback(wallet)
            
            # Process transfers (deposits/withdrawals)
            transfers_data = data.get("transfers", [])
            for transfer_data in transfers_data:
                transfer = self._parse_transfer(transfer_data)
                if transfer:
                    self.transfers.append(transfer)
                    if len(self.transfers) > 50:
                        self.transfers = self.transfers[-50:]
                    logger.info(f"Transfer: {transfer.type} {transfer.amount} {transfer.asset} [{transfer.status}]")
                    if self.on_transfer_callback:
                        self.on_transfer_callback(transfer)
            
            # Keep legacy storage bounded
            if len(self.account_updates) > 100:
                self.account_updates = self.account_updates[-100:]
            if len(self.order_updates) > 100:
                self.order_updates = self.order_updates[-100:]

        elif isinstance(data, list):
            for item in data:
                self._process_account_update(item)
    
    def _parse_order(self, data: dict) -> Optional[Order]:
        """Parse order data from APEX WebSocket."""
        try:
            return Order(
                id=str(data.get("id", "")),
                symbol=data.get("symbol", ""),
                side=data.get("side", ""),
                type=data.get("type", ""),
                status=data.get("status", ""),
                price=data.get("price", "0"),
                size=data.get("size", "0"),
                remaining_size=data.get("remainingSize", "0"),
                cum_fill_size=data.get("cumSuccessFillSize", "0"),
                cum_fill_value=data.get("cumSuccessFillValue", "0"),
                cum_fill_fee=data.get("cumSuccessFillFee", "0"),
                client_id=data.get("clientId", data.get("clientOrderId", "")),
                time_in_force=str(data.get("timeInForce", "")),
                reduce_only=data.get("reduceOnly", False),
                trigger_price=data.get("triggerPrice", ""),
                created_at=data.get("createdAt", 0),
                updated_at=data.get("updatedAt", 0)
            )
        except Exception as e:
            logger.error(f"Parse order error: {e}")
            return None
    
    def _parse_fill(self, data: dict) -> Optional[Fill]:
        """Parse fill/trade data from APEX WebSocket."""
        try:
            return Fill(
                id=str(data.get("id", "")),
                order_id=str(data.get("orderId", "")),
                symbol=data.get("symbol", ""),
                side=data.get("side", ""),
                price=data.get("price", "0"),
                size=data.get("size", "0"),
                fee=data.get("fee", "0"),
                liquidity=data.get("liquidity", "UNKNOWN"),
                quote_amount=data.get("quoteAmount", "0"),
                is_open=data.get("isOpen", True),
                created_at=data.get("createdAt", 0),
                updated_at=data.get("updatedAt", 0)
            )
        except Exception as e:
            logger.error(f"Parse fill error: {e}")
            return None
    
    def _parse_position(self, data: dict) -> Optional[Position]:
        """Parse position data from APEX WebSocket."""
        try:
            return Position(
                symbol=data.get("symbol", ""),
                side=data.get("side", ""),
                size=data.get("size", "0"),
                entry_price=data.get("entryPrice", "0"),
                exit_price=data.get("exitPrice", "0"),
                realized_pnl=data.get("realizedPnl", "0"),
                funding_fee=data.get("fundingFee", "0"),
                sum_open=data.get("sumOpen", "0"),
                sum_close=data.get("sumClose", "0"),
                open_value=data.get("openValue", "0"),
                custom_imr=data.get("customImr", "0"),
                updated_at=data.get("updatedAt", 0)
            )
        except Exception as e:
            logger.error(f"Parse position error: {e}")
            return None
    
    def _parse_wallet(self, data: dict) -> Optional[Wallet]:
        """Parse wallet/balance data from APEX WebSocket."""
        try:
            return Wallet(
                token=data.get("token", data.get("asset", "USDT")),
                balance=data.get("balance", "0"),
                pending_deposit=data.get("pendingDepositAmount", "0"),
                pending_withdraw=data.get("pendingWithdrawAmount", "0"),
                pending_transfer_in=data.get("pendingTransferInAmount", "0"),
                pending_transfer_out=data.get("pendingTransferOutAmount", "0")
            )
        except Exception as e:
            logger.error(f"Parse wallet error: {e}")
            return None
    
    def _parse_transfer(self, data: dict) -> Optional[Transfer]:
        """Parse transfer (deposit/withdrawal) data from APEX WebSocket."""
        try:
            return Transfer(
                id=str(data.get("id", "")),
                type=data.get("type", "UNKNOWN"),
                status=data.get("status", "UNKNOWN"),
                asset=data.get("creditAsset", ""),
                amount=data.get("creditAmount", "0"),
                transaction_id=str(data.get("transactionId", "")),
                transaction_hash=data.get("transactionHash", "") or "",
                created_at=data.get("createdAt", 0),
                confirmed_at=data.get("confirmedAt", 0) or 0
            )
        except Exception as e:
            logger.error(f"Parse transfer error: {e}")
            return None

    def _process_notification(self, data: Any):
        """Process notification message from ws_notify_v1 topic."""
        if isinstance(data, dict):
            self.notifications.append(data)
            
            # Parse notification list
            notify_list = data.get("notifyMsgList", []) + data.get("notify_list", [])
            for notify_data in notify_list:
                notification = self._parse_notification(notify_data)
                if notification:
                    self.notifications_list.append(notification)
                    if len(self.notifications_list) > 50:
                        self.notifications_list = self.notifications_list[-50:]
                    logger.info(f"Notification: {notification.title}")
                    if self.on_notification_callback:
                        self.on_notification_callback(notification)
            
            # Keep only last 50 notifications
            if len(self.notifications) > 50:
                self.notifications = self.notifications[-50:]
    
    def _parse_notification(self, data: dict) -> Optional[Notification]:
        """Parse notification message."""
        try:
            return Notification(
                id=str(data.get("id", "")),
                category=str(data.get("category", "")),
                title=data.get("title", ""),
                content=data.get("content", ""),
                read=data.get("read", False),
                created_time=data.get("createdTime", 0)
            )
        except Exception as e:
            logger.error(f"Parse notification error: {e}")
            return None

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
        logger.info("Disconnecting APEX private WebSocket...")
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
        return self.connected

    def is_authenticated(self) -> bool:
        """Check authentication status."""
        return self.authenticated

    def get_latest_order_update(self) -> Optional[Dict[str, Any]]:
        """Get the most recent order update (raw dict)."""
        return self.order_updates[-1] if self.order_updates else None

    def get_latest_account_update(self) -> Optional[Dict[str, Any]]:
        """Get the most recent account update (raw dict)."""
        return self.account_updates[-1] if self.account_updates else None
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID."""
        return self.orders.get(order_id)
    
    def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        return [o for o in self.orders.values() if o.status in ("OPEN", "PENDING", "UNTRIGGERED")]
    
    def get_position(self, symbol: str, side: str = "LONG") -> Optional[Position]:
        """Get position by symbol and side."""
        return self.positions.get(f"{symbol}_{side}")
    
    def get_all_positions(self) -> List[Position]:
        """Get all positions with non-zero size."""
        return [p for p in self.positions.values() if float(p.size) > 0]
    
    def get_wallet(self, token: str = "USDT") -> Optional[Wallet]:
        """Get wallet balance by token."""
        return self.wallets.get(token)
    
    def get_balance(self, token: str = "USDT") -> float:
        """Get balance for a token, returns 0.0 if not found."""
        wallet = self.wallets.get(token)
        return float(wallet.balance) if wallet else 0.0
    
    def get_latest_fills(self, count: int = 10) -> List[Fill]:
        """Get most recent fills."""
        return self.fills[-count:] if self.fills else []

    def clear_updates(self):
        """Clear stored updates."""
        self.order_updates.clear()
        self.account_updates.clear()
        self.notifications.clear()
        self.orders.clear()
        self.positions.clear()
        self.wallets.clear()
        self.fills.clear()
        self.transfers.clear()
        self.notifications_list.clear()
