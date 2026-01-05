"""
Database Stats Viewer - View your trading statistics from the SQLite database.
"""
import sqlite3
import os
from datetime import datetime, timedelta

def main():
    db_path = os.getenv('DB_PATH', 'orders.db')
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("📊 Trading Database Statistics")
    print("=" * 70)
    
    # 1. Total Orders Count
    print("\n📈 Order Summary")
    print("-" * 50)
    try:
        cursor.execute("SELECT COUNT(*) FROM completed_orders")
        total = cursor.fetchone()[0]
        print(f"   Total Recorded Orders: {total}")
        
        cursor.execute("SELECT COUNT(*) FROM completed_orders WHERE side='Bid'")
        buys = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM completed_orders WHERE side='Ask'")
        sells = cursor.fetchone()[0]
        print(f"   Buy Orders: {buys}")
        print(f"   Sell Orders: {sells}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 2. Volume Statistics
    print("\n📊 Volume Statistics")
    print("-" * 50)
    try:
        cursor.execute("SELECT SUM(quantity), SUM(quantity * price), SUM(fee) FROM completed_orders")
        row = cursor.fetchone()
        total_qty = row[0] or 0
        total_volume = row[1] or 0
        total_fees = row[2] or 0
        print(f"   Total Quantity: {total_qty:.6f} ETH")
        print(f"   Total Volume: ${total_volume:,.2f} USDT")
        print(f"   Total Fees: ${total_fees:.4f} USDT")
        
        # Maker vs Taker
        cursor.execute("SELECT SUM(quantity), SUM(quantity * price) FROM completed_orders WHERE maker=1")
        maker = cursor.fetchone()
        maker_qty = maker[0] or 0
        maker_vol = maker[1] or 0
        
        cursor.execute("SELECT SUM(quantity), SUM(quantity * price) FROM completed_orders WHERE maker=0")
        taker = cursor.fetchone()
        taker_qty = taker[0] or 0
        taker_vol = taker[1] or 0
        
        print(f"\n   Maker Volume: ${maker_vol:,.2f} USDT ({maker_qty:.4f} ETH)")
        print(f"   Taker Volume: ${taker_vol:,.2f} USDT ({taker_qty:.4f} ETH)")
        if total_volume > 0:
            maker_pct = (maker_vol / total_volume) * 100
            print(f"   Maker Ratio: {maker_pct:.1f}%")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 3. Recent Trades
    print("\n📜 Recent Trades (Last 10)")
    print("-" * 50)
    try:
        cursor.execute("""
            SELECT side, quantity, price, maker, fee, timestamp 
            FROM completed_orders 
            ORDER BY timestamp DESC 
            LIMIT 10
        """)
        trades = cursor.fetchall()
        if trades:
            for i, trade in enumerate(trades, 1):
                side, qty, price, maker, fee, ts = trade
                maker_str = "MAKER" if maker else "TAKER"
                print(f"   {i}. {side:4} {qty:.4f} @ {price:.2f} ({maker_str}) Fee: {fee:.6f} | {ts}")
        else:
            print("   No trades recorded yet.")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 4. Daily Stats
    print("\n📅 Daily Statistics")
    print("-" * 50)
    try:
        cursor.execute("""
            SELECT date, 
                   maker_buy_volume + maker_sell_volume + taker_buy_volume + taker_sell_volume as total_vol,
                   total_fees,
                   net_profit,
                   trade_count
            FROM trading_stats 
            ORDER BY date DESC 
            LIMIT 7
        """)
        stats = cursor.fetchall()
        if stats:
            for date, vol, fees, profit, count in stats:
                print(f"   {date}: Vol={vol:.4f} ETH, Fees=${fees:.4f}, Net=${profit:.4f}, Trades={count}")
        else:
            print("   No daily stats recorded yet.")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 5. Symbol breakdown
    print("\n🪙 Per-Symbol Breakdown")
    print("-" * 50)
    try:
        cursor.execute("""
            SELECT symbol, COUNT(*), SUM(quantity), SUM(quantity * price), SUM(fee)
            FROM completed_orders 
            GROUP BY symbol
        """)
        symbols = cursor.fetchall()
        if symbols:
            for sym, count, qty, vol, fees in symbols:
                qty = qty or 0
                vol = vol or 0
                fees = fees or 0
                print(f"   {sym}: {count} orders, {qty:.4f} base, ${vol:,.2f} volume, ${fees:.4f} fees")
        else:
            print("   No data by symbol yet.")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 70)
    print("Database Stats Complete")
    print("=" * 70)
    
    conn.close()

if __name__ == "__main__":
    main()
