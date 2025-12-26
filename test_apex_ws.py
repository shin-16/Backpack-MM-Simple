"""
APEX Public WebSocket Test Script (Separated Handlers)

Usage:
    python test_apex_ws.py                       # Depth only (default)
    python test_apex_ws.py --ticker              # Ticker only
    python test_apex_ws.py --depth               # Depth only
    python test_apex_ws.py --trades              # Trades only
    python test_apex_ws.py --alltickers          # All tickers (all symbols)
    python test_apex_ws.py --packetloss          # Monitor packet loss only
    python test_apex_ws.py --marketstatus        # Show market status summary
    python test_apex_ws.py --depth --store-db    # Store depth to TimescaleDB
    python test_apex_ws.py ETHUSDT --depth       # Custom symbol
"""
import sys
import time
import argparse
from ws_client.apex_client import ApexWebSocket


class ApexDataHandler:
    """Handler class that separates ticker, depth, and trade updates."""
    
    def __init__(self, show_ticker=False, show_depth=False, show_trades=False, 
                 show_packetloss=False, show_marketstatus=False, show_alltickers=False,
                 depth_writer=None, symbol=None):
        self.show_ticker = show_ticker
        self.show_depth = show_depth
        self.show_trades = show_trades
        self.show_packetloss = show_packetloss
        self.show_marketstatus = show_marketstatus
        self.show_alltickers = show_alltickers
        self.depth_writer = depth_writer
        self.symbol = symbol
        
        # All tickers data (dict of symbol -> data)
        self.all_tickers = {}
        
        # Ticker data
        self.last_price = None
        self.index_price = None
        self.funding_rate = None
        self.price_24h_pcnt = None
        self.high_price_24h = None
        self.low_price_24h = None
        self.volume_24h = None
        self.turnover_24h = None
        self.open_interest = None
        self.oracle_price = None
        self.next_funding_time = None
        self.predicted_funding_rate = None
        self.trade_count = None
        
        # Orderbook data
        self.best_bid = None
        self.best_ask = None
        self.bid_depth = []
        self.ask_depth = []
        
        # Trade data
        self.last_trade_price = None
        self.last_trade_size = None
        self.last_trade_side = None
        
        # Packet loss tracking
        self.depth_updates_received = 0
    
    def on_ticker(self, data: dict):
        """Handle ticker updates with all available fields."""
        # Price fields
        if "lastPrice" in data:
            self.last_price = float(data["lastPrice"])
        if "indexPrice" in data:
            self.index_price = float(data["indexPrice"])
        if "oraclePrice" in data:
            self.oracle_price = float(data["oraclePrice"])
        
        # 24h stats
        if "price24hPcnt" in data:
            self.price_24h_pcnt = float(data["price24hPcnt"]) * 100  # Convert to percentage
        if "highPrice24h" in data:
            self.high_price_24h = float(data["highPrice24h"])
        if "lowPrice24h" in data:
            self.low_price_24h = float(data["lowPrice24h"])
        if "volume24h" in data:
            self.volume_24h = float(data["volume24h"])
        if "turnover24h" in data:
            self.turnover_24h = float(data["turnover24h"])
        
        # Funding
        if "fundingRate" in data:
            self.funding_rate = float(data["fundingRate"])
        if "predictedFundingRate" in data:
            self.predicted_funding_rate = float(data["predictedFundingRate"])
        if "nextFundingTime" in data:
            self.next_funding_time = data["nextFundingTime"]
        
        # Other
        if "openInterest" in data:
            self.open_interest = float(data["openInterest"])
        if "tradeCount" in data:
            self.trade_count = int(data["tradeCount"])
        
        # Print if enabled
        if self.show_ticker:
            self._print_ticker_update(data)
    
    def _print_ticker_update(self, data: dict):
        """Print formatted ticker update in APEX API order."""
        parts = []
        
        # 1. price24hPcnt - 24H change (%)
        if "price24hPcnt" in data:
            sign = "+" if self.price_24h_pcnt >= 0 else ""
            parts.append(f"24h%: {sign}{self.price_24h_pcnt:.2f}%")
        
        # 2. lastPrice - Last price
        if "lastPrice" in data:
            parts.append(f"LastPrice: ${self.last_price}")
        
        # 3. highPrice24h - 24H highest price
        if "highPrice24h" in data:
            parts.append(f"High24h: ${self.high_price_24h}")
        
        # 4. lowPrice24h - 24H lowest price
        if "lowPrice24h" in data:
            parts.append(f"Low24h: ${self.low_price_24h}")
        
        # Oracle Price is not sent from APEX API
        # # 5. oraclePrice - Oracle price
        # if "oraclePrice" in data:
        #     parts.append(f"Oracle: ${self.oracle_price}")
        
        # 6. indexPrice - Index price
        if "indexPrice" in data:
            parts.append(f"Index: ${self.index_price}")
        
        # 7. openInterest - Open interest
        if "openInterest" in data:
            parts.append(f"OI: {self.open_interest}")
        
        # 8. turnover24h - 24H turnover
        if "turnover24h" in data:
            parts.append(f"Turnover24h: ${self.turnover_24h:.2f}")
        
        # 9. volume24h - 24H trading volume
        if "volume24h" in data:
            parts.append(f"Vol24h: {self.volume_24h:.2f}")
        
        # 10. fundingRate - Funding rate
        if "fundingRate" in data:
            parts.append(f"FundingRate: {self.funding_rate}")
        
        # 11. predictedFundingRate - Predicted funding rate
        if "predictedFundingRate" in data:
            parts.append(f"PredFunding: {self.predicted_funding_rate}")
        
        # 12. nextFundingTime - Next funding time
        if "nextFundingTime" in data:
            parts.append(f"NextFunding: {self.next_funding_time}")
        
        # 13. tradeCount - 24H trade count
        if "tradeCount" in data:
            parts.append(f"TradeCount: {self.trade_count}")
        
        if parts:
            print(f"[Ticker] {' | '.join(parts)}")
    
    def on_depth(self, data: dict):
        """Handle orderbook depth updates."""
        self.depth_updates_received += 1
        
        bids = data.get("b", [])
        asks = data.get("a", [])
        update_id = data.get("u")
        msg_type = data.get("type", "delta")
        
        # Store to database if writer is configured
        if self.depth_writer and self.symbol:
            self.depth_writer.write_batch(
                symbol=self.symbol,
                bids=bids,
                asks=asks,
                update_id=update_id,
                msg_type=msg_type
            )
        
        if bids:
            valid_bids = [b for b in bids if float(b[1]) > 0]
            if valid_bids:
                self.best_bid = float(valid_bids[0][0])
                self.bid_depth = valid_bids[:5]
                if self.show_depth:
                    print(f"[Depth] Bids: {valid_bids[:3]}")
        
        if asks:
            valid_asks = [a for a in asks if float(a[1]) > 0]
            if valid_asks:
                self.best_ask = float(valid_asks[0][0])
                self.ask_depth = valid_asks[:5]
                if self.show_depth:
                    print(f"[Depth] Asks: {valid_asks[:3]}")
    
    def on_trade(self, data):
        """Handle trade updates with all fields."""
        if isinstance(data, list):
            for trade in data:
                if isinstance(trade, dict):
                    self.last_trade_price = float(trade.get("p", 0))
                    self.last_trade_size = float(trade.get("v", 0))
                    self.last_trade_side = trade.get("S", "")
                    trade_id = trade.get("i", "")  # Trade ID/UUID
                    trade_time = trade.get("T", "")  # Timestamp
                    
                    if self.show_trades:
                        # Format timestamp if available
                        time_str = ""
                        if trade_time:
                            try:
                                from datetime import datetime
                                # APEX timestamp is in milliseconds
                                dt = datetime.fromtimestamp(int(trade_time) / 1000)
                                time_str = dt.strftime("%H:%M:%S.%f")[:-3]
                            except:
                                time_str = str(trade_time)
                        
                        parts = [f"{self.last_trade_side} {self.last_trade_size} @ ${self.last_trade_price}"]
                        if time_str:
                            parts.append(f"Time: {time_str}")
                        if trade_id:
                            parts.append(f"ID: {trade_id}")
                        
                        print(f"[Trade] {' | '.join(parts)}")
    
    def handle_message(self, topic: str, data: dict):
        """Route messages to appropriate handler."""
        if topic == "instrumentInfo.all":
            self.on_all_tickers(data)
        elif topic.startswith("instrumentInfo."):
            self.on_ticker(data)
        elif topic.startswith("orderBook"):
            self.on_depth(data)
        elif topic.startswith("recentlyTrade."):
            self.on_trade(data)
    
    def on_all_tickers(self, data):
        """Handle all tickers updates.
        
        Field mapping (compact -> full):
            s  = symbol
            p  = lastPrice
            pr = price24hPcnt (24h change %)
            h  = highPrice24h
            l  = lowPrice24h
            op = oraclePrice
            xp = indexPrice
            to = turnover24h
            v  = volume24h
            fr = fundingRate
            o  = openInterest
            tc = tradeCount
        """
        if isinstance(data, list):
            for ticker in data:
                if isinstance(ticker, dict) and "s" in ticker:
                    symbol = ticker["s"]
                    # Update stored data
                    if symbol not in self.all_tickers:
                        self.all_tickers[symbol] = {}
                    
                    # Map compact fields to full names
                    field_map = {
                        "p": "lastPrice",
                        "pr": "price24hPcnt",
                        "h": "highPrice24h",
                        "l": "lowPrice24h",
                        "op": "oraclePrice",
                        "xp": "indexPrice",
                        "to": "turnover24h",
                        "v": "volume24h",
                        "fr": "fundingRate",
                        "o": "openInterest",
                        "tc": "tradeCount"
                    }
                    
                    for compact, full in field_map.items():
                        if compact in ticker:
                            self.all_tickers[symbol][full] = ticker[compact]
                    
                    if self.show_alltickers:
                        self._print_all_ticker_update(symbol, ticker)
    
    def _print_all_ticker_update(self, symbol: str, ticker: dict):
        """Print formatted all-ticker update."""
        parts = [f"{symbol}"]
        
        if "p" in ticker:
            parts.append(f"${ticker['p']}")
        if "pr" in ticker:
            pct = float(ticker["pr"]) * 100
            sign = "+" if pct >= 0 else ""
            parts.append(f"{sign}{pct:.2f}%")
        if "v" in ticker:
            parts.append(f"Vol: {ticker['v']}")
        if "xp" in ticker:
            parts.append(f"Idx: ${ticker['xp']}")
        if "fr" in ticker:
            parts.append(f"FR: {ticker['fr']}")
        
        print(f"[AllTickers] {' | '.join(parts)}")
    
    def print_all_tickers_summary(self):
        """Print summary of all tickers."""
        if not self.all_tickers:
            print("No tickers received yet.")
            return
        
        print("\n" + "=" * 80)
        print("                          ALL TICKERS SUMMARY")
        print("=" * 80)
        print(f"{'Symbol':<12} {'Price':>12} {'24h %':>10} {'Volume':>15} {'OI':>12}")
        print("-" * 80)
        
        for symbol in sorted(self.all_tickers.keys()):
            data = self.all_tickers[symbol]
            price = data.get("lastPrice", "-")
            pct = data.get("price24hPcnt", None)
            if pct:
                pct_val = float(pct) * 100
                pct_str = f"{'+' if pct_val >= 0 else ''}{pct_val:.2f}%"
            else:
                pct_str = "-"
            vol = data.get("volume24h", "-")
            oi = data.get("openInterest", "-")
            print(f"{symbol:<12} ${price:>11} {pct_str:>10} {vol:>15} {oi:>12}")
        
        print("=" * 80)
        print(f"Total symbols: {len(self.all_tickers)}")
        print("=" * 80 + "\n")
    
    def get_spread(self) -> float:
        if self.best_bid and self.best_ask:
            return ((self.best_ask - self.best_bid) / self.best_bid) * 100
        return 0.0
    
    def print_marketstatus(self):
        """Print market status summary."""
        print("\n" + "=" * 50)
        print("         CURRENT MARKET STATUS")
        print("=" * 50)
        print(f"  Last Price  : ${self.last_price}")
        print(f"  Best Bid    : ${self.best_bid}")
        print(f"  Best Ask    : ${self.best_ask}")
        print(f"  Spread      : {self.get_spread():.4f}%")
        print(f"  Index Price : ${self.index_price}")
        print(f"  Funding Rate: {self.funding_rate}")
        print("=" * 50 + "\n")
    
    def print_packetloss(self, ws):
        """Print packet loss monitoring info."""
        print("\n" + "=" * 50)
        print("         PACKET LOSS MONITOR")
        print("=" * 50)
        print(f"  Last Update ID    : {ws.last_update_id}")
        print(f"  Depth Updates     : {self.depth_updates_received}")
        print(f"  Packet Losses     : {ws.packet_loss_count}")
        print(f"  Snapshot Received : {ws.depth_snapshot_received}")
        if self.depth_updates_received > 0:
            loss_rate = (ws.packet_loss_count / self.depth_updates_received) * 100
            print(f"  Loss Rate         : {loss_rate:.2f}%")
        if ws.packet_loss_count > 0:
            print(f"  ⚠️  PACKET LOSS DETECTED!")
        else:
            print(f"  ✅ No packet loss")
        print("=" * 50 + "\n")
    
    def print_periodic(self, ws):
        """Print periodic summary based on enabled flags."""
        if self.show_marketstatus:
            self.print_marketstatus()
        if self.show_packetloss:
            self.print_packetloss(ws)


def main():
    parser = argparse.ArgumentParser(description="APEX WebSocket Test")
    parser.add_argument("symbol", nargs="?", default="AAVEUSDT", help="Trading pair (e.g., BTCUSDT)")
    parser.add_argument("--ticker", action="store_true", help="Subscribe to ticker updates")
    parser.add_argument("--depth", action="store_true", help="Subscribe to depth updates")
    parser.add_argument("--trades", action="store_true", help="Subscribe to trade updates")
    parser.add_argument("--alltickers", action="store_true", help="Subscribe to all tickers (all symbols)")
    parser.add_argument("--packetloss", action="store_true", help="Monitor packet loss")
    parser.add_argument("--marketstatus", action="store_true", help="Show periodic market status summary")
    parser.add_argument("--store-db", action="store_true", help="Store depth data to TimescaleDB")
    parser.add_argument("--db-url", default="postgresql://postgres:password@localhost:5432/apex", 
                        help="TimescaleDB connection URL")
    args = parser.parse_args()
    
    # If only packetloss flag, monitor packet loss silently (with depth)
    packetloss_only = args.packetloss and not args.ticker and not args.depth and not args.trades and not args.marketstatus and not args.alltickers
    
    # If only alltickers flag, just show all tickers
    alltickers_only = args.alltickers and not args.ticker and not args.depth and not args.trades and not args.marketstatus and not args.packetloss
    
    # If no filter flags, default to depth only
    if not args.ticker and not args.depth and not args.trades and not args.packetloss and not args.marketstatus and not args.alltickers:
        show_ticker = False
        show_depth = True
        show_trades = False
        show_packetloss = False
        show_marketstatus = False
        show_alltickers = False
    else:
        show_ticker = args.ticker
        show_depth = args.depth or packetloss_only  # depth needed for packet loss
        show_trades = args.trades
        show_packetloss = args.packetloss
        show_marketstatus = args.marketstatus
        show_alltickers = args.alltickers
    
    symbol = args.symbol.upper()
    
    print("=" * 50)
    print(f"APEX WebSocket Test - {symbol}")
    filters = []
    if show_ticker: filters.append("ticker")
    if show_depth or packetloss_only: filters.append("depth")
    if show_trades: filters.append("trades")
    if show_alltickers: filters.append("alltickers")
    if show_packetloss: filters.append("packetloss")
    if show_marketstatus: filters.append("marketstatus")
    if args.store_db: filters.append("store-db")
    print(f"Showing: {', '.join(filters) if filters else 'depth (default)'}")
    print("=" * 50)
    
    # Initialize database if --store-db is enabled
    db = None
    depth_writer = None
    if args.store_db:
        try:
            from database.timescale_db import TimescaleDB
            from database.depth_writer import DepthWriter
            
            print(f"\nConnecting to TimescaleDB: {args.db_url}")
            db = TimescaleDB(args.db_url)
            db.init_schema()
            
            depth_writer = DepthWriter(db)
            depth_writer.start()
            print("Database writer started.")
        except ImportError as e:
            print(f"Error: Missing dependency. Run: pip install psycopg2-binary")
            print(f"Details: {e}")
            return
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return
    
    # Create handler with filters
    handler = ApexDataHandler(
        show_ticker=show_ticker,
        show_depth=show_depth if not packetloss_only else False,
        show_trades=show_trades,
        show_packetloss=show_packetloss or packetloss_only,
        show_marketstatus=show_marketstatus,
        show_alltickers=show_alltickers,
        depth_writer=depth_writer,
        symbol=symbol
    )
    
    # Create WebSocket client
    ws = ApexWebSocket(
        symbol=symbol,
        on_message_callback=handler.handle_message,
        auto_reconnect=True
    )
    
    print("\nConnecting...")
    ws.connect()
    
    for i in range(10):
        time.sleep(1)
        if ws.connected:
            print("Connected!\n")
            break
        print(f"Waiting... {i+1}s")
    
    if not ws.connected:
        print("Failed to connect!")
        return
    
    # Subscribe to optional streams
    # Note: ticker and depth are subscribed by default in apex_client._on_open()
    if show_trades:
        ws.subscribe_trades()
    
    if show_alltickers:
        ws.subscribe_all_tickers()
    
    print("-" * 50)
    print("Receiving data (Ctrl+C to stop)...")
    print("-" * 50 + "\n")
    
    try:
        while True:
            time.sleep(5)
            if alltickers_only:
                handler.print_all_tickers_summary()
            else:
                handler.print_periodic(ws)
            
    except KeyboardInterrupt:
        print("\n\nStopping...")
    
    # Final all tickers summary
    if show_alltickers:
        handler.print_all_tickers_summary()
    
    # Final packet loss summary
    if show_packetloss or packetloss_only:
        print("\n" + "=" * 50)
        print("         FINAL PACKET LOSS SUMMARY")
        print("=" * 50)
        print(f"  Total Depth Updates : {handler.depth_updates_received}")
        print(f"  Total Packet Losses : {ws.packet_loss_count}")
        if handler.depth_updates_received > 0:
            loss_rate = (ws.packet_loss_count / handler.depth_updates_received) * 100
            print(f"  Total Loss Rate     : {loss_rate:.2f}%")
        print("=" * 50)
    
    ws.disconnect()
    
    # Stop depth writer and show stats
    if depth_writer:
        depth_writer.stop()
        stats = depth_writer.get_stats()
        print("\n" + "=" * 50)
        print("         DATABASE WRITE SUMMARY")
        print("=" * 50)
        print(f"  Records Queued  : {stats['records_queued']}")
        print(f"  Records Written : {stats['records_written']}")
        print(f"  Batches Written : {stats['batches_written']}")
        print(f"  Errors          : {stats['errors']}")
        print("=" * 50)
    
    if db:
        db.close()
    
    print("Done!")


if __name__ == "__main__":
    main()
