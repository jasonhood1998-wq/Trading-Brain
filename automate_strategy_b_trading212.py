"""
===============================================================================
STRATEGY B (PULLBACK TREND-FOLLOWING) AUTOMATED TRADING 212 EXECUTION ENGINE
===============================================================================
Author: Finance Student Assistant (Antigravity)
Account: Trading 212 Demo Practice Account (Invest / Stocks ISA)
===============================================================================
"""

import os
import sys
import time
import base64
import datetime
import zoneinfo
import requests
import pandas as pd 
import yfinance as yf
import pandas_market_calendars as mcal
from dotenv import load_dotenv

# 1. Environment & Path Setup
load_dotenv()
load_dotenv(dotenv_path=os.path.expanduser("~/.env"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_LOG_PATH = os.path.join(SCRIPT_DIR, "Strategy_B_Trade_Log.csv")
AUDIT_LOG_PATH = os.path.join(SCRIPT_DIR, "Strategy_B_Scan_Audit.csv")

API_KEY = os.getenv("TRADING212_API_KEY")
API_SECRET = os.getenv("TRADING212_API_SECRET")
IS_DEMO = os.getenv("TRADING212_DEMO", "true").lower() == "true"

BASE_URL = "https://demo.trading212.com/api/v0" if IS_DEMO else "https://live.trading212.com/api/v0"

def get_headers():
    credentials = f"{API_KEY}:{API_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json"
    }

def init_log_file():
    """Ensures the CSV trade log file exists with headers before scanning."""
    if not os.path.exists(TRADE_LOG_PATH):
        with open(TRADE_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Status,Ticker,Category,Date,State,Shares,Entry,Stop,Target,PnL,Return,Fees,Notes\n")

def init_scan_audit_log():
    """Creates a dedicated CSV log to record every stock scan and rejection reason."""
    if not os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Timestamp,Ticker,Price,Trend_Pass,Pullback_Pass,Reversal_Pass,Decision,Reason\n")

def log_scan_result(ticker: str, price: float, trend_pass: bool, pullback_pass: bool, reversal_pass: bool, decision: str, reason: str):
    """Appends scan results to CSV and forces immediate disk flushing."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f'"{now_str}","{ticker}","{price:.2f}","{trend_pass}","{pullback_pass}","{reversal_pass}","{decision}","{reason}"\n'
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(log_line)
            f.flush()
            os.fsync(f.fileno())  # Force OS to write buffer to disk instantly
        print(f"   [AUDIT WRITTEN] {ticker} -> Decision: {decision}")
    except Exception as e:
        print(f"[ERROR] Failed writing to audit log: {e}")

# 2. Market Open & Holiday Guard
def is_market_open() -> bool:
    try:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_et = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        today_str = now_et.strftime('%Y-%m-%d')

        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today_str, end_date=today_str)

        if schedule.empty:
            print(f"[SKIP] Today ({today_str}) is a weekend or US stock market holiday.")
            return False

        market_open = schedule.iloc[0]['market_open'].to_pydatetime()
        market_close = schedule.iloc[0]['market_close'].to_pydatetime()

        if market_open <= now_utc < market_close:
            return True
        else:
            print(f"[SKIP] Outside market hours. Today's NYSE schedule: {market_open.strftime('%H:%M')} - {market_close.strftime('%H:%M')} UTC")
            return False
    except Exception as e:
        print(f"[ERROR] Failed to check market hours: {e}")
        return False

# 3. Trading 212 Pre-Flight Diagnostics & Order Testing
def test_trading212_connection() -> bool:
    """Pre-flight diagnostic check: Verifies credentials, endpoint, and API permissions."""
    print("\n=======================================================================")
    print("[PRE-FLIGHT 1/2] RUNNING TRADING 212 API DIAGNOSTIC TEST")
    print("=======================================================================")
    print(f"[*] Target Environment: {'DEMO' if IS_DEMO else 'LIVE'}")
    print(f"[*] Base URL: {BASE_URL}")
    print(f"[*] API Key Present: {'YES' if API_KEY else 'NO (Missing TRADING212_API_KEY)'}")
    print(f"[*] API Secret Present: {'YES' if API_SECRET else 'NO (Missing TRADING212_API_SECRET)'}")

    if not API_KEY:
        print("[CRITICAL] Environment variable TRADING212_API_KEY is not set.")
        return False

    url = f"{BASE_URL}/equity/account/summary"
    try:
        res = requests.get(url, headers=get_headers(), timeout=10)
        print(f"[*] HTTP Status Code: {res.status_code}")

        if res.status_code == 200:
            data = res.json()
            print(" SUCCESS: Connected to Trading 212 API!")
            print(f"   Account ID: {data.get('id')}")
            print(f"   Total Value: GBP {data.get('totalValue', 0.0):,.2f}")
            print(f"   Available Cash: GBP {data.get('free', 0.0):,.2f}")
            print("=======================================================================\n")
            return True
        elif res.status_code == 401:
            print(" [401 Unauthorized] Invalid API Key/Secret or incorrect Base URL.")
        elif res.status_code == 403:
            print(" [403 Forbidden] Key exists but lacks required permissions (Account/Orders).")
        else:
            print(f" [API Error] {res.text}")
    except Exception as e:
        print(f" [Network Exception] Failed to reach Trading 212 servers: {e}")

    print("=======================================================================\n")
    return False

def test_paper_order_execution(ticker: str = "AAPL_US_EQ") -> bool:
    """Safely tests order placement and cancellation on Trading 212 Demo."""
    if not IS_DEMO:
        print("[SAFETY] Order test aborted. This test must ONLY run in DEMO mode!")
        return False

    print("=======================================================================")
    print("[PRE-FLIGHT 2/2] RUNNING PAPER TRADING ORDER PLACEMENT & CANCEL TEST")
    print("=======================================================================")

    place_url = f"{BASE_URL}/equity/orders/limit"
    payload = {
        "ticker": ticker,
        "quantity": 1.0,
        "limitPrice": 1.00,
        "timeValidity": "DAY"
    }

    try:
        print(f"[*] Placing safe test limit order: BUY 1 share of {ticker} @ $1.00...")
        res = requests.post(place_url, headers=get_headers(), json=payload, timeout=10)
        
        if res.status_code not in (200, 201):
            print(f"[FAIL] Order placement failed ({res.status_code}): {res.text}")
            print("[HINT] Ensure your API Key has 'Orders' permissions enabled in Trading 212 settings.")
            return False

        order_data = res.json()
        order_id = order_data.get("id") or order_data.get("orderId")
        print(f" SUCCESS: Test order placed with ID: {order_id}")

        time.sleep(1)
        cancel_url = f"{BASE_URL}/equity/orders/{order_id}"
        print(f"[*] Canceling test order {order_id}...")
        
        cancel_res = requests.delete(cancel_url, headers=get_headers(), timeout=10)
        if cancel_res.status_code in (200, 204):
            print(" SUCCESS: Test order canceled successfully!")
            print("=======================================================================\n")
            return True
        else:
            print(f"[WARNING] Order placed, but cancellation failed ({cancel_res.status_code}): {cancel_res.text}")
            return False

    except Exception as e:
        print(f"[ERROR] Exception during paper order test: {e}")
        return False

# 4. API Helpers
def fetch_account_summary():
    try:
        url = f"{BASE_URL}/equity/account/summary"
        res = requests.get(url, headers=get_headers(), timeout=10)
        if res.status_code == 200:
            return res.json()
        print(f"[ERROR] Account Summary API Failed ({res.status_code}): {res.text}")
    except Exception as e:
        print(f"[ERROR] Request exception in fetch_account_summary: {e}")
    return None

def fetch_open_positions():
    try:
        url = f"{BASE_URL}/equity/positions"
        res = requests.get(url, headers=get_headers(), timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"[ERROR] Request exception in fetch_open_positions: {e}")
    return []

def fetch_pending_orders():
    try:
        url = f"{BASE_URL}/equity/orders"
        res = requests.get(url, headers=get_headers(), timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"[ERROR] Request exception in fetch_pending_orders: {e}")
    return []

def place_market_order(ticker: str, quantity: float):
    try:
        url = f"{BASE_URL}/equity/orders/market"
        payload = {
            "ticker": ticker,
            "quantity": quantity
        }
        res = requests.post(url, headers=get_headers(), json=payload, timeout=10)
        return res.status_code, res.json()
    except Exception as e:
        print(f"[ERROR] Market order placement failed for {ticker}: {e}")
        return 500, {"error": str(e)}

def convert_t212_to_yf(t212_ticker: str) -> str:
    if t212_ticker.endswith("_US_EQ"):
        return t212_ticker.replace("_US_EQ", "")
    return t212_ticker

# 5. Execution Engine
def run_strategy_b_automation(watchlist, risk_pct=0.01):
    print("=======================================================================")
    print("[BOT] STARTING STRATEGY B AUTOMATED TRADING 212 EXECUTION ENGINE")
    print("=======================================================================")
    
    init_log_file()
    init_scan_audit_log()

    summary = fetch_account_summary()
    if not summary:
        print("[CRITICAL] Could not connect to Trading 212 API. Aborting scan.")
        return

    account_val = summary.get("totalValue", 5000.0)
    cash_available = summary.get("cash", {}).get("availableToTrade", 5000.0)
    risk_amount_gbp = account_val * risk_pct

    print(f"[*] Account ID: {summary.get('id')}")
    print(f"[*] Account Total Equity: GBP {account_val:,.2f}")
    print(f"[*] Available Cash: GBP {cash_available:,.2f}")
    print(f"[*] 1.0% Risk Per Trade Limit: GBP {risk_amount_gbp:,.2f}")
    print("-----------------------------------------------------------------------")

    open_pos = fetch_open_positions()
    active_tickers = [p.get("ticker") for p in open_pos]
    print(f"[*] Current Open Positions ({len(active_tickers)}): {active_tickers}")

    pending_orders = fetch_pending_orders()
    print(f"[*] Current Pending Orders: {len(pending_orders)}")
    print("-----------------------------------------------------------------------")

    for ticker in watchlist:
        time.sleep(0.5)  # API rate-limit buffer
        
        if ticker in active_tickers:
            print(f"[SKIP] {ticker}: Position already open.")
            log_scan_result(ticker, 0.0, False, False, False, "NO_INVESTMENT", "Position already open")
            continue

        yf_symbol = convert_t212_to_yf(ticker)
        
        try:
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period="5d", interval="5m")
            
            if df.empty or len(df) < 50:
                print(f"[SKIP] {ticker} ({yf_symbol}): Insufficient market data.")
                log_scan_result(ticker, 0.0, False, False, False, "NO_INVESTMENT", "Insufficient market data")
                continue

            # Indicator Calculations
            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['SMA50'] = df['Close'].rolling(window=50).mean()
            
            df['TR'] = df[['High', 'Low', 'Close']].apply(
                lambda x: max(x['High'] - x['Low'], abs(x['High'] - df['Close'].shift(1).loc[x.name]), abs(x['Low'] - df['Close'].shift(1).loc[x.name])), 
                axis=1
            )
            df['ATR14'] = df['TR'].rolling(window=14).mean()

            closed_bar = df.iloc[-2]
            recent_bars = df.iloc[-6:-1]

            # Strategy Rules
            trend_pass = bool(closed_bar['EMA20'] > closed_bar['SMA50'])
            pullback_pass = bool((recent_bars['Low'] <= (recent_bars['EMA20'] * 1.001)).any())
            reversal_pass = bool(closed_bar['Close'] > closed_bar['Open'])

            current_close = float(closed_bar['Close'])
            current_low = float(closed_bar['Low'])
            ema20 = float(closed_bar['EMA20'])
            sma50 = float(closed_bar['SMA50'])
            atr14 = float(closed_bar['ATR14'])

            print(f"\n[SCAN] Ticker: {ticker} ({yf_symbol})")
            print(f"   Price: ${current_close:.2f} | 20 EMA: ${ema20:.2f} | 50 SMA: ${sma50:.2f} | ATR(14): ${atr14:.2f}")
            print(f"   Filters -> Trend (EMA>SMA): {trend_pass} | Pullback (5-bar lookback): {pullback_pass} | Reversal: {reversal_pass}")

            if trend_pass and pullback_pass and reversal_pass:
                print(f"--> [SIGNAL TRIGGERED] Strategy B Buy Signal on {ticker}!")
                
                entry_price = current_close
                stop_price = current_low - (1.5 * atr14)
                risk_per_share = entry_price - stop_price
                target_price = entry_price + (2.5 * risk_per_share)

                if risk_per_share > 0.05:
                    usd_gbp_rate = 1.28
                    risk_amount_usd = risk_amount_gbp * usd_gbp_rate
                    shares = round(risk_amount_usd / risk_per_share, 4)

                    if shares >= 0.0001:
                        print(f"   Calculated Order: BUY {shares} shares of {ticker} @ ${entry_price:.2f}")
                        
                        status_code, response_data = place_market_order(ticker, shares)
                        print(f"   [API RESPONSE] Status: {status_code} | Output: {response_data}")

                        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                        log_line = f'"LIVE","{ticker}","US MegaCap","{now_str}","OPEN","{shares}","{entry_price:.2f}","{stop_price:.2f}","{target_price:.2f}","0.00","0.00","0.00","LIVE DEMO ENTRY"\n'
                        with open(TRADE_LOG_PATH, "a", encoding="utf-8") as f:
                            f.write(log_line)
                            f.flush()

                        log_scan_result(ticker, current_close, trend_pass, pullback_pass, reversal_pass, "INVESTED", "All filters passed - Order placed")
                    else:
                        print("   [SKIP] Calculated share size < 0.0001 shares.")
                        log_scan_result(ticker, current_close, trend_pass, pullback_pass, reversal_pass, "NO_INVESTMENT", "Share size too small (<0.0001)")
            else:
                reasons = []
                if not trend_pass:
                    reasons.append("Failed Trend (EMA <= SMA)")
                if not pullback_pass:
                    reasons.append("Failed Pullback (No touch in 5 bars)")
                if not reversal_pass:
                    reasons.append("Failed Reversal (Bar closed red)")
                
                reason_str = " | ".join(reasons)
                print(f"   [NO INVESTMENT] Reason: {reason_str}")
                log_scan_result(ticker, current_close, trend_pass, pullback_pass, reversal_pass, "NO_INVESTMENT", reason_str)

        except Exception as e:
            print(f"[ERROR] Failed processing {ticker}: {e}")
            log_scan_result(ticker, 0.0, False, False, False, "ERROR", f"Processing exception: {e}")

    print("\n=======================================================================")
    print("[BOT] STRATEGY B AUTOMATION SCAN COMPLETED")
    print(f"[INFO] Audit log location: {AUDIT_LOG_PATH}")
    print("=======================================================================")

# 6. Single Run Main Execution Block (Optimized for CI/CD Workflow Runners)
if __name__ == "__main__":
    init_log_file()
    init_scan_audit_log()

    # Pre-Flight Diagnostic Tests
    if not test_trading212_connection():
        print("[CRITICAL] API connection diagnostic failed. Stopping execution.")
        sys.exit(1)

    if not test_paper_order_execution():
        print("[CRITICAL] Paper order permission test failed. Check API key permissions.")
        sys.exit(1)

    watchlist = [
        "NVDA_US_EQ", "AAPL_US_EQ", "MSFT_US_EQ", "TSLA_US_EQ", 
        "AMZN_US_EQ", "GOOGL_US_EQ", "AMD_US_EQ", "META_US_EQ"
    ]

    # Perform a single scan execution per job trigger
    if is_market_open():
        run_strategy_b_automation(watchlist)
    else:
        print("[STANDBY] Market is closed. Skipping scan cycle.")

    print("[COMPLETED] Script execution finished successfully.")
    sys.exit(0)
