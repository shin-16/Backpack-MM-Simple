"""
Zoomex Exchange REST API Client Module

Implements the Zoomex V3 API for perpetual futures trading.
API is similar to Bybit V5 API.
"""
import json
import time
import hmac
import hashlib
import requests
from typing import Dict, Any, List, Optional, Tuple
from logger import setup_logger
from .base_client import BaseExchangeClient
from .proxy_utils import get_proxy_config

logger = setup_logger("api.zoomex")


class ZoomexClient(BaseExchangeClient):
    """Zoomex exchange client (REST).

    Implements Zoomex V3 API for USDT perpetual futures trading.
    Authentication uses HMAC-SHA256 with X-BAPI-* headers.
    """

    # API endpoints
    DEFAULT_BASE_URL = "https://openapi.zoomex.com"
    TESTNET_BASE_URL = "https://openapi-testnet.zoomex.com"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config.get("api_key", "")
        self.api_secret = config.get("api_secret", "") or config.get("secret_key", "")
        self.base_url = config.get("base_url", self.DEFAULT_BASE_URL)
        self.recv_window = config.get("recv_window", "5000")
        self.category = config.get("category", "linear")  # linear, inverse, spot

        # Proxy configuration
        self.proxies = get_proxy_config()
        if self.proxies:
            logger.info(f"Zoomex client configured with proxy: {self.proxies}")

    def get_exchange_name(self) -> str:
        return "Zoomex"

    async def connect(self) -> None:
        logger.info("Zoomex client connected")

    async def disconnect(self) -> None:
        logger.info("Zoomex client disconnected")

    # ------------------------------------------------------------------
    # Signature Generation
    # ------------------------------------------------------------------

    def _generate_signature(self, timestamp: str, payload: str) -> str:
        """Generate HMAC-SHA256 signature for Zoomex API.
        
        Signature format: HMAC_SHA256(timestamp + api_key + recv_window + payload)
        """
        param_str = timestamp + self.api_key + self.recv_window + payload
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def _get_auth_headers(self, payload: str = "") -> Dict[str, str]:
        """Generate authentication headers for Zoomex API."""
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, payload)
        
        return {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }

    # ------------------------------------------------------------------
    # HTTP Request Methods
    # ------------------------------------------------------------------

    def make_request(self, method: str, endpoint: str, api_key=None, secret_key=None,
                     instruction=None, params=None, data=None, retry_count: int = 3) -> Dict:
        """Execute HTTP request with retry logic.
        
        Args:
            method: HTTP method (GET, POST, DELETE)
            endpoint: API endpoint
            api_key: Not used (kept for interface compatibility)
            secret_key: Not used (kept for interface compatibility)
            instruction: Not used (kept for interface compatibility)
            params: Query parameters (for GET requests)
            data: Request body data (for POST requests)
            retry_count: Number of retries
            
        Returns:
            API response as dict
        """
        url = f"{self.base_url}{endpoint}"
        
        # Build payload string for signature
        if method.upper() == "GET" and params:
            payload = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        elif method.upper() == "POST" and data:
            payload = json.dumps(data, separators=(',', ':'))
        else:
            payload = ""

        # Get authentication headers
        headers = self._get_auth_headers(payload)

        # Add query params to URL for GET requests
        if method.upper() == "GET" and params:
            url += "?" + payload

        # Retry logic
        for attempt in range(retry_count):
            try:
                if method.upper() == "GET":
                    response = requests.get(
                        url, headers=headers,
                        proxies=self.proxies or None,
                        timeout=10
                    )
                elif method.upper() == "POST":
                    response = requests.post(
                        url, headers=headers,
                        data=payload if data else None,
                        proxies=self.proxies or None,
                        timeout=10
                    )
                elif method.upper() == "DELETE":
                    response = requests.delete(
                        url, headers=headers,
                        data=payload if data else None,
                        proxies=self.proxies or None,
                        timeout=10
                    )
                else:
                    return {"error": f"Unsupported HTTP method: {method}"}

                # Parse response
                if response.status_code in [200, 201]:
                    result = response.json() if response.text.strip() else {}
                    
                    # Check for API-level errors
                    if result.get("retCode", 0) != 0:
                        error_msg = result.get("retMsg", "Unknown error")
                        return {"error": f"API error: {error_msg}", "raw": result}
                    
                    return result
                    
                elif response.status_code == 429:  # Rate limit
                    wait_time = 1 * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"Status: {response.status_code}, Message: {response.text}"
                    if attempt < retry_count - 1:
                        logger.warning(f"Request failed ({attempt+1}/{retry_count}): {error_msg}")
                        time.sleep(1)
                        continue
                    return {"error": error_msg}

            except requests.exceptions.Timeout:
                if attempt < retry_count - 1:
                    logger.warning(f"Timeout ({attempt+1}/{retry_count}), retrying...")
                    continue
                return {"error": "Request timeout"}
            except requests.exceptions.ConnectionError:
                if attempt < retry_count - 1:
                    logger.warning(f"Connection error ({attempt+1}/{retry_count}), retrying...")
                    time.sleep(2)
                    continue
                return {"error": "Connection error"}
            except Exception as e:
                if attempt < retry_count - 1:
                    logger.warning(f"Exception ({attempt+1}/{retry_count}): {str(e)}, retrying...")
                    continue
                return {"error": f"Request failed: {str(e)}"}

        return {"error": "Max retries reached"}

    # ------------------------------------------------------------------
    # Market Data Methods (Public)
    # ------------------------------------------------------------------

    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker information for a symbol.
        
        Endpoint: GET /cloud/trade/v3/market/tickers
        """
        endpoint = "/cloud/trade/v3/market/tickers"
        params = {
            "category": self.category,
            "symbol": symbol
        }
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        # Parse ticker data
        result_data = response.get("result", {})
        ticker_list = result_data.get("list", [])
        
        if not ticker_list:
            return {"error": "No ticker data returned"}

        ticker = ticker_list[0]
        return {
            "symbol": ticker.get("symbol"),
            "lastPrice": ticker.get("lastPrice"),
            "bidPrice": ticker.get("bid1Price"),
            "askPrice": ticker.get("ask1Price"),
            "volume": ticker.get("volume24h"),
            "change24h": ticker.get("price24hPcnt"),
            "highPrice24h": ticker.get("highPrice24h"),
            "lowPrice24h": ticker.get("lowPrice24h"),
            "markPrice": ticker.get("markPrice"),
            "indexPrice": ticker.get("indexPrice"),
            "raw": ticker
        }

    def get_order_book(self, symbol: str, limit: int = 25) -> Dict:
        """Get orderbook depth for a symbol.
        
        Endpoint: GET /cloud/trade/v3/market/orderbook
        """
        endpoint = "/cloud/trade/v3/market/orderbook"
        params = {
            "category": self.category,
            "symbol": symbol,
            "limit": str(limit)
        }
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        
        # Parse bids and asks: [[price, qty], ...]
        bids_raw = result.get("b", [])
        asks_raw = result.get("a", [])
        
        bids = [[float(b[0]), float(b[1])] for b in bids_raw]
        asks = [[float(a[0]), float(a[1])] for a in asks_raw]
        
        return {
            "symbol": result.get("s"),
            "bids": bids,
            "asks": asks,
            "timestamp": result.get("ts"),
            "updateId": result.get("u")
        }

    def get_markets(self) -> Dict:
        """Get all available trading instruments.
        
        Endpoint: GET /cloud/trade/v3/market/instruments-info
        """
        endpoint = "/cloud/trade/v3/market/instruments-info"
        params = {"category": self.category}
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        return {
            "category": result.get("category"),
            "list": result.get("list", [])
        }

    # ------------------------------------------------------------------
    # Account Methods (Private)
    # ------------------------------------------------------------------

    def get_balance(self) -> Dict:
        """Get wallet balance.
        
        Endpoint: GET /cloud/trade/v3/account/wallet-balance
        """
        endpoint = "/cloud/trade/v3/account/wallet-balance"
        params = {"accountType": "UNIFIED"}  # or CONTRACT for classic
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        account_list = result.get("list", [])
        
        if not account_list:
            return {"balances": [], "raw": result}

        account = account_list[0]
        coins = account.get("coin", [])
        
        balances = []
        for coin in coins:
            balances.append({
                "asset": coin.get("coin"),
                "available": coin.get("availableToWithdraw"),
                "total": coin.get("walletBalance"),
                "equity": coin.get("equity"),
                "unrealizedPnl": coin.get("unrealisedPnl")
            })
        
        return {
            "accountType": account.get("accountType"),
            "balances": balances,
            "totalEquity": account.get("totalEquity"),
            "totalAvailableBalance": account.get("totalAvailableBalance"),
            "raw": account
        }

    def get_positions(self, symbol: Optional[str] = None) -> Dict:
        """Get position information.
        
        Endpoint: GET /cloud/trade/v3/position/list
        """
        endpoint = "/cloud/trade/v3/position/list"
        params = {"category": self.category}
        
        if symbol:
            params["symbol"] = symbol
        else:
            params["settleCoin"] = "USDT"  # Get all USDT positions
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        position_list = result.get("list", [])
        
        positions = []
        for pos in position_list:
            size = float(pos.get("size", 0))
            if size > 0:  # Only include non-zero positions
                positions.append({
                    "symbol": pos.get("symbol"),
                    "side": pos.get("side"),
                    "size": size,
                    "entryPrice": pos.get("avgPrice"),
                    "markPrice": pos.get("markPrice"),
                    "unrealizedPnl": pos.get("unrealisedPnl"),
                    "leverage": pos.get("leverage"),
                    "liquidationPrice": pos.get("liqPrice"),
                    "positionValue": pos.get("positionValue"),
                    "raw": pos
                })
        
        return {"positions": positions, "raw": result}

    # ------------------------------------------------------------------
    # Order Methods (Private)
    # ------------------------------------------------------------------

    def execute_order(self, order_details: Dict) -> Dict:
        """Place a new order.
        
        Endpoint: POST /cloud/trade/v3/order/create
        
        Args:
            order_details: Dict with keys:
                - symbol: Trading pair (e.g., "BTCUSDT")
                - side: "Buy" or "Sell"
                - orderType: "Market" or "Limit"
                - qty: Order quantity
                - price: Order price (required for limit orders)
                - timeInForce: "GTC", "IOC", "FOK", "PostOnly"
                - positionIdx: 0 (one-way), 1 (hedge-buy), 2 (hedge-sell)
                - orderLinkId: Custom order ID (optional)
        """
        endpoint = "/cloud/trade/v3/order/create"
        
        # Build order payload
        data = {
            "category": self.category,
            "symbol": order_details.get("symbol"),
            "side": order_details.get("side"),
            "orderType": order_details.get("orderType", "Limit"),
            "qty": str(order_details.get("qty")),
            "positionIdx": order_details.get("positionIdx", 0)
        }
        
        # Optional fields
        if order_details.get("price"):
            data["price"] = str(order_details["price"])
        
        if order_details.get("timeInForce"):
            data["timeInForce"] = order_details["timeInForce"]
        else:
            data["timeInForce"] = "GTC"
        
        if order_details.get("orderLinkId"):
            data["orderLinkId"] = order_details["orderLinkId"]
        
        if order_details.get("reduceOnly"):
            data["reduceOnly"] = order_details["reduceOnly"]
        
        response = self.make_request("POST", endpoint, data=data)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        return {
            "orderId": result.get("orderId"),
            "orderLinkId": result.get("orderLinkId"),
            "success": True,
            "raw": result
        }

    def cancel_order(self, order_id: str, symbol: str) -> Dict:
        """Cancel a specific order.
        
        Endpoint: POST /cloud/trade/v3/order/cancel
        """
        endpoint = "/cloud/trade/v3/order/cancel"
        data = {
            "category": self.category,
            "symbol": symbol,
            "orderId": order_id
        }
        
        response = self.make_request("POST", endpoint, data=data)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        return {
            "orderId": result.get("orderId"),
            "orderLinkId": result.get("orderLinkId"),
            "success": True,
            "raw": result
        }

    def cancel_all_orders(self, symbol: str = None) -> Dict:
        """Cancel all open orders.
        
        Endpoint: POST /cloud/trade/v3/order/cancel-all
        """
        endpoint = "/cloud/trade/v3/order/cancel-all"
        data = {"category": self.category}
        
        if symbol:
            data["symbol"] = symbol
        else:
            data["settleCoin"] = "USDT"  # Cancel all USDT perpetual orders
        
        response = self.make_request("POST", endpoint, data=data)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        return {
            "success": True,
            "list": result.get("list", []),
            "raw": result
        }

    def get_open_orders(self, symbol: Optional[str] = None) -> Dict:
        """Get open orders.
        
        Endpoint: GET /cloud/trade/v3/order/realtime
        """
        endpoint = "/cloud/trade/v3/order/realtime"
        params = {
            "category": self.category,
            "openOnly": "0"  # Open orders only
        }
        
        if symbol:
            params["symbol"] = symbol
        else:
            params["settleCoin"] = "USDT"
        
        response = self.make_request("GET", endpoint, params=params)
        
        if "error" in response:
            return response

        result = response.get("result", {})
        order_list = result.get("list", [])
        
        orders = []
        for order in order_list:
            orders.append({
                "orderId": order.get("orderId"),
                "orderLinkId": order.get("orderLinkId"),
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "orderType": order.get("orderType"),
                "price": order.get("price"),
                "qty": order.get("qty"),
                "filledQty": order.get("cumExecQty"),
                "status": order.get("orderStatus"),
                "timeInForce": order.get("timeInForce"),
                "createdTime": order.get("createdTime"),
                "raw": order
            })
        
        return {"orders": orders, "raw": result}

    # ------------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------------

    def get_market_limits(self, symbol: str) -> Optional[Dict]:
        """Get trading limits for a symbol (min order size, tick size, etc.)."""
        markets = self.get_markets()
        
        if "error" in markets:
            logger.error(f"Failed to get markets: {markets}")
            return None

        for instrument in markets.get("list", []):
            if instrument.get("symbol") == symbol:
                lot_filter = instrument.get("lotSizeFilter", {})
                price_filter = instrument.get("priceFilter", {})
                
                # Extract base and quote from symbol (e.g., BTCUSDT -> BTC, USDT)
                # Zoomex uses settleCoin as quote asset indicator
                quote_coin = instrument.get("settleCoin", "USDT")
                base_coin = instrument.get("baseCoin", symbol.replace(quote_coin, ""))
                
                # Calculate precision from step sizes
                qty_step = lot_filter.get("qtyStep", "0.001")
                tick_size = price_filter.get("tickSize", "0.01")
                
                # Count decimal places for precision
                base_precision = len(qty_step.split('.')[-1]) if '.' in qty_step else 0
                quote_precision = len(tick_size.split('.')[-1]) if '.' in tick_size else 0
                
                return {
                    "symbol": symbol,
                    "base_asset": base_coin,
                    "quote_asset": quote_coin,
                    "base_precision": base_precision,
                    "quote_precision": quote_precision,
                    "min_order_size": float(lot_filter.get("minOrderQty", "0.001")),
                    "max_order_size": float(lot_filter.get("maxOrderQty", "1000000")),
                    "tick_size": float(tick_size),
                    "qty_step": float(qty_step),
                    "raw": instrument
                }
        
        logger.error(f"Symbol {symbol} not found in markets")
        return None

    def get_deposit_address(self, blockchain: str) -> Dict:
        """Not implemented for Zoomex - returns error."""
        return {"error": "Deposit address query not supported for Zoomex"}

    def get_collateral(self, subaccount_id=None) -> Dict:
        """Get collateral info - alias for get_balance."""
        return self.get_balance()
