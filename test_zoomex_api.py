"""
Test script to verify Zoomex API integration and metrics collection.
Run this to check if balance, orders, and fills are being retrieved correctly.
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env file
from dotenv import load_dotenv
load_dotenv()

from api.zoomex_client import ZoomexClient
from logger import setup_logger

logger = setup_logger("test_zoomex")

def main():
    # Load config from environment (match names used in server.py)
    api_key = os.getenv("ZOOMEX_API_KEY", "")
    secret_key = os.getenv("ZOOMEX_API_SECRET", "")  # Note: API_SECRET not SECRET_KEY
    
    if not api_key or not secret_key:
        print("❌ Error: ZOOMEX_API_KEY and ZOOMEX_API_SECRET environment variables required")
        print("   Check your .env file has these variables set.")
        return
    
    config = {
        "api_key": api_key,
        "secret_key": secret_key
    }
    
    print("=" * 60)
    print("Zoomex API Integration Test")
    print("=" * 60)
    
    client = ZoomexClient(config)
    symbol = "ETHUSDT"
    
    # Test 1: Get Balance
    print("\n📊 Test 1: Get Balance")
    print("-" * 40)
    try:
        balance = client.get_balance()
        if isinstance(balance, dict) and "error" in balance:
            print(f"   ❌ Error: {balance['error']}")
        else:
            print(f"   ✓ Balance retrieved successfully")
            for asset, data in balance.items():
                if isinstance(data, dict):
                    available = data.get('available', 0)
                    total = data.get('total', 0)
                    print(f"   {asset}: Available={available}, Total={total}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 2: Get Symbol Info
    print("\n📊 Test 2: Get Symbol Info")
    print("-" * 40)
    try:
        info = client.get_symbol_info(symbol)
        if info:
            print(f"   ✓ Symbol info retrieved for {symbol}")
            print(f"   Base: {info.get('base_asset')}, Quote: {info.get('quote_asset')}")
            print(f"   Tick Size: {info.get('tick_size')}")
            print(f"   Min Order: {info.get('min_order_size')}")
            print(f"   Base Precision: {info.get('base_precision')}")
        else:
            print(f"   ❌ No info returned")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 3: Get Ticker
    print("\n📊 Test 3: Get Ticker")
    print("-" * 40)
    try:
        ticker = client.get_ticker(symbol)
        if isinstance(ticker, dict) and "error" in ticker:
            print(f"   ❌ Error: {ticker['error']}")
        else:
            print(f"   ✓ Ticker retrieved")
            print(f"   Last Price: {ticker.get('lastPrice')}")
            print(f"   Bid: {ticker.get('bid1Price')}, Ask: {ticker.get('ask1Price')}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 4: Get Open Orders
    print("\n📊 Test 4: Get Open Orders")
    print("-" * 40)
    try:
        orders = client.get_open_orders(symbol)
        if isinstance(orders, dict) and "error" in orders:
            print(f"   ❌ Error: {orders['error']}")
        elif isinstance(orders, list):
            print(f"   ✓ Open orders retrieved: {len(orders)} orders")
            for i, order in enumerate(orders[:3]):  # Show first 3
                print(f"   Order {i+1}:")
                print(f"     ID: {order.get('id')}")
                print(f"     OrderId: {order.get('orderId')}")
                print(f"     Side: {order.get('side')}, Price: {order.get('price')}")
        else:
            print(f"   ⚠ Unexpected format: {type(orders)}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 5: Get Fill History
    print("\n📊 Test 5: Get Fill History")
    print("-" * 40)
    try:
        fills = client.get_fill_history(symbol, limit=10)
        if isinstance(fills, dict) and "error" in fills:
            print(f"   ❌ Error: {fills['error']}")
        elif isinstance(fills, list):
            print(f"   ✓ Fill history retrieved: {len(fills)} fills")
            for i, fill in enumerate(fills[:5]):  # Show first 5
                print(f"   Fill {i+1}:")
                print(f"     Fill ID: {fill.get('fill_id')}")
                print(f"     Order ID: {fill.get('order_id')}")
                print(f"     Side: {fill.get('side')}, Qty: {fill.get('quantity')}, Price: {fill.get('price')}")
                print(f"     Fee: {fill.get('fee')}, PnL: {fill.get('realized_pnl')}")
        else:
            print(f"   ⚠ Unexpected format: {type(fills)}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 6: Get Positions
    print("\n📊 Test 6: Get Positions")
    print("-" * 40)
    try:
        positions = client.get_positions(symbol)
        if isinstance(positions, dict) and "error" in positions:
            print(f"   ❌ Error: {positions['error']}")
        elif isinstance(positions, list):
            print(f"   ✓ Positions retrieved: {len(positions)} positions")
            for i, pos in enumerate(positions[:3]):
                print(f"   Position {i+1}:")
                print(f"     Symbol: {pos.get('symbol')}")
                print(f"     Size: {pos.get('size')}, Side: {pos.get('side')}")
                print(f"     Entry: {pos.get('entryPrice')}, Mark: {pos.get('markPrice')}")
                print(f"     Unrealized PnL: {pos.get('unrealizedPnl')}")
        else:
            print(f"   ⚠ Unexpected format: {type(positions)}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
