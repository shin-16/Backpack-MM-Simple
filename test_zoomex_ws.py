#!/usr/bin/env python
"""
Zoomex WebSocket Test Script

Tests the Zoomex public WebSocket client for orderbook and ticker data.
Similar to test_apex_ws.py.

Usage:
    python test_zoomex_ws.py [--symbol SYMBOL] [--depth] [--testnet]

Options:
    --symbol SYMBOL    Trading pair symbol (default: BTCUSDT)
    --depth            Display orderbook depth
    --testnet          Use testnet endpoint
"""
import argparse
import signal
import sys
import time
from typing import Dict, Any

from ws_client.zoomex_client import ZoomexWebSocket
from logger import setup_logger

logger = setup_logger("test_zoomex")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\nShutting down...")
    sys.exit(0)


def format_price(price: float, decimals: int = 2) -> str:
    """Format price with proper decimal places."""
    if price is None:
        return "N/A"
    return f"{price:,.{decimals}f}"


def format_orderbook(orderbook: Dict[str, list], levels: int = 5) -> str:
    """Format orderbook for display."""
    lines = []
    lines.append("=" * 50)
    lines.append(f"{'Price':>15} | {'Quantity':>15} | {'Side':>8}")
    lines.append("-" * 50)
    
    # Asks (sell orders) - show top levels reversed so lowest ask is at bottom
    asks = orderbook.get("asks", [])[:levels]
    for ask in reversed(asks):
        try:
            price = float(ask[0])
            qty = float(ask[1])
            lines.append(f"{format_price(price):>15} | {qty:>15.4f} | {'ASK':>8}")
        except (ValueError, IndexError):
            pass
    
    lines.append("-" * 50)
    
    # Bids (buy orders)
    bids = orderbook.get("bids", [])[:levels]
    for bid in bids:
        try:
            price = float(bid[0])
            qty = float(bid[1])
            lines.append(f"{format_price(price):>15} | {qty:>15.4f} | {'BID':>8}")
        except (ValueError, IndexError):
            pass
    
    lines.append("=" * 50)
    return "\n".join(lines)


def on_message(topic: str, data: Dict[str, Any]):
    """Callback for WebSocket messages."""
    logger.debug(f"Received: {topic}")


def main():
    parser = argparse.ArgumentParser(description="Test Zoomex WebSocket client")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair symbol")
    parser.add_argument("--depth", action="store_true", help="Display orderbook depth")
    parser.add_argument("--testnet", action="store_true", help="Use testnet endpoint")
    args = parser.parse_args()

    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)

    print(f"\n{'=' * 60}")
    print(f"  Zoomex WebSocket Test")
    print(f"  Symbol: {args.symbol}")
    print(f"  Endpoint: {'Testnet' if args.testnet else 'Mainnet'}")
    print(f"{'=' * 60}\n")

    # Create WebSocket client
    ws_client = ZoomexWebSocket(
        symbol=args.symbol,
        on_message_callback=on_message,
        testnet=args.testnet
    )

    # Connect
    print("Connecting to Zoomex WebSocket...")
    ws_client.connect()

    # Wait for connection
    timeout = 10
    start = time.time()
    while not ws_client.is_connected() and time.time() - start < timeout:
        time.sleep(0.5)

    if not ws_client.is_connected():
        print("Failed to connect!")
        return

    print("Connected!\n")

    # Main loop
    try:
        while True:
            # Clear screen (optional)
            # print("\033[H\033[J", end="")
            
            # Display ticker info
            print(f"\r{args.symbol} | ", end="")
            print(f"Last: {format_price(ws_client.last_price)} | ", end="")
            print(f"Bid: {format_price(ws_client.bid_price)} | ", end="")
            print(f"Ask: {format_price(ws_client.ask_price)} | ", end="")
            if ws_client.mark_price:
                print(f"Mark: {format_price(ws_client.mark_price)} | ", end="")
            if ws_client.funding_rate:
                print(f"Funding: {ws_client.funding_rate*100:.4f}%", end="")
            print("    ", end="", flush=True)
            
            # Display orderbook if requested
            if args.depth and ws_client.orderbook.get("bids"):
                print("\n")
                print(format_orderbook(ws_client.orderbook))
            
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        print("\nDisconnecting...")
        ws_client.disconnect()
        print("Done.")


if __name__ == "__main__":
    main()
