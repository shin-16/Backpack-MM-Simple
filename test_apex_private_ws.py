"""
APEX Private WebSocket Test Script

Usage:
    python test_apex_private_ws.py                    # Show all updates
    python test_apex_private_ws.py --orders           # Show order updates only
    python test_apex_private_ws.py --fills            # Show fills only
    python test_apex_private_ws.py --positions        # Show positions only
    python test_apex_private_ws.py --balance          # Show balance changes only
    python test_apex_private_ws.py --notifications    # Show notifications only

Requires environment variables:
    APEX_API_KEY
    APEX_SECRET_KEY
    APEX_PASSPHRASE
"""
import sys
import os
import time
import argparse
from ws_client.apex_private_client import (
    ApexPrivateWebSocket,
    Order, Fill, Position, Wallet, Transfer, Notification
)


class PrivateWSHandler:
    """Handler for Private WebSocket events."""
    
    def __init__(self, show_orders=True, show_fills=True, show_positions=True,
                 show_balance=True, show_notifications=True):
        self.show_orders = show_orders
        self.show_fills = show_fills
        self.show_positions = show_positions
        self.show_balance = show_balance
        self.show_notifications = show_notifications
        
        # Counters
        self.order_count = 0
        self.fill_count = 0
        self.position_count = 0
        self.wallet_count = 0
        self.notification_count = 0
    
    def on_order(self, order: Order):
        """Handle order update."""
        self.order_count += 1
        if self.show_orders:
            status_emoji = {
                "PENDING": "⏳",
                "OPEN": "📖",
                "FILLED": "✅",
                "CANCELED": "❌",
                "UNTRIGGERED": "⏸️"
            }.get(order.status, "❓")
            
            print(f"[Order] {status_emoji} {order.symbol} {order.side} {order.size} @ {order.price} | "
                  f"Status: {order.status} | ID: {order.id[:12]}...")
    
    def on_fill(self, fill: Fill):
        """Handle fill/trade."""
        self.fill_count += 1
        if self.show_fills:
            liquidity_emoji = "🏷️" if fill.liquidity == "MAKER" else "🎯"
            print(f"[Fill] {liquidity_emoji} {fill.symbol} {fill.side} {fill.size} @ {fill.price} | "
                  f"Fee: {fill.fee} | {fill.liquidity}")
    
    def on_position(self, position: Position):
        """Handle position update."""
        self.position_count += 1
        if self.show_positions:
            side_emoji = "🟢" if position.side == "LONG" else "🔴"
            if float(position.size) > 0:
                print(f"[Position] {side_emoji} {position.symbol} {position.side} | "
                      f"Size: {position.size} | Entry: {position.entry_price} | "
                      f"PnL: {position.realized_pnl}")
    
    def on_wallet(self, wallet: Wallet):
        """Handle wallet/balance update."""
        self.wallet_count += 1
        if self.show_balance:
            print(f"[Wallet] 💰 {wallet.token}: {wallet.balance} | "
                  f"Pending: +{wallet.pending_deposit} -{wallet.pending_withdraw}")
    
    def on_transfer(self, transfer: Transfer):
        """Handle transfer (deposit/withdrawal)."""
        if self.show_balance:
            emoji = "📥" if transfer.type == "DEPOSIT" else "📤"
            print(f"[Transfer] {emoji} {transfer.type} {transfer.amount} {transfer.asset} | "
                  f"Status: {transfer.status}")
    
    def on_notification(self, notification: Notification):
        """Handle notification."""
        self.notification_count += 1
        if self.show_notifications:
            print(f"[Notification] 🔔 {notification.title}: {notification.content[:50]}...")
    
    def print_summary(self, ws: ApexPrivateWebSocket):
        """Print status summary."""
        print("\n" + "=" * 60)
        print("              PRIVATE WEBSOCKET STATUS")
        print("=" * 60)
        print(f"  Connected    : {'✅' if ws.connected else '❌'}")
        print(f"  Authenticated: {'✅' if ws.authenticated else '❌'}")
        print(f"  Orders       : {self.order_count} received | {len(ws.get_open_orders())} open")
        print(f"  Fills        : {self.fill_count} received | {len(ws.fills)} stored")
        print(f"  Positions    : {self.position_count} updates | {len(ws.get_all_positions())} active")
        print(f"  Wallet       : {ws.get_balance('USDT'):.2f} USDT")
        print(f"  Notifications: {self.notification_count} received")
        print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="APEX Private WebSocket Test")
    parser.add_argument("--orders", action="store_true", help="Show order updates only")
    parser.add_argument("--fills", action="store_true", help="Show fills only")
    parser.add_argument("--positions", action="store_true", help="Show position updates only")
    parser.add_argument("--balance", action="store_true", help="Show balance/wallet changes only")
    parser.add_argument("--notifications", action="store_true", help="Show notifications only")
    args = parser.parse_args()
    
    # Get credentials from environment
    api_key = os.getenv("APEX_API_KEY")
    secret_key = os.getenv("APEX_SECRET_KEY")
    passphrase = os.getenv("APEX_PASSPHRASE", "")
    
    if not api_key or not secret_key:
        print("Error: Missing credentials!")
        print("Set environment variables:")
        print("  APEX_API_KEY=your_api_key")
        print("  APEX_SECRET_KEY=your_secret_key")
        print("  APEX_PASSPHRASE=your_passphrase (optional)")
        return
    
    # Determine what to show
    any_filter = args.orders or args.fills or args.positions or args.balance or args.notifications
    if any_filter:
        show_orders = args.orders
        show_fills = args.fills
        show_positions = args.positions
        show_balance = args.balance
        show_notifications = args.notifications
    else:
        # No filters = show everything
        show_orders = True
        show_fills = True
        show_positions = True
        show_balance = True
        show_notifications = True
    
    print("=" * 60)
    print("        APEX Private WebSocket Test")
    print("=" * 60)
    filters = []
    if show_orders: filters.append("orders")
    if show_fills: filters.append("fills")
    if show_positions: filters.append("positions")
    if show_balance: filters.append("balance")
    if show_notifications: filters.append("notifications")
    print(f"Showing: {', '.join(filters)}")
    print("=" * 60)
    
    # Create handler
    handler = PrivateWSHandler(
        show_orders=show_orders,
        show_fills=show_fills,
        show_positions=show_positions,
        show_balance=show_balance,
        show_notifications=show_notifications
    )
    
    # Create WebSocket client
    ws = ApexPrivateWebSocket(
        api_key=api_key,
        secret_key=secret_key,
        passphrase=passphrase,
        auto_reconnect=True
    )
    
    # Set event callbacks
    ws.on_order_callback = handler.on_order
    ws.on_fill_callback = handler.on_fill
    ws.on_position_callback = handler.on_position
    ws.on_wallet_callback = handler.on_wallet
    ws.on_transfer_callback = handler.on_transfer
    ws.on_notification_callback = handler.on_notification
    
    print("\nConnecting...")
    ws.connect()
    
    # Wait for connection and authentication
    for i in range(15):
        time.sleep(1)
        if ws.authenticated:
            print("Authenticated! ✅\n")
            break
        elif ws.connected:
            print(f"Connected, authenticating... {i+1}s")
        else:
            print(f"Connecting... {i+1}s")
    
    if not ws.authenticated:
        print("Failed to authenticate!")
        print("Check your API credentials.")
        ws.disconnect()
        return
    
    print("-" * 60)
    print("Receiving updates (Ctrl+C to stop)...")
    print("-" * 60 + "\n")
    
    try:
        while True:
            time.sleep(10)
            handler.print_summary(ws)
            
    except KeyboardInterrupt:
        print("\n\nStopping...")
    
    # Final summary
    handler.print_summary(ws)
    
    ws.disconnect()
    print("Done!")


if __name__ == "__main__":
    main()
