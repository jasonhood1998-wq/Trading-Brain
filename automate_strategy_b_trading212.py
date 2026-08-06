"""
===============================================================================
STRATEGY B (PULLBACK TREND-FOLLOWING) AUTOMATED TRADING 212 EXECUTION ENGINE
===============================================================================
Author: Finance Student Assistant & Senior Quant Engineering
Account: Trading 212 Practice / Live Account (Invest / Stocks ISA)
===============================================================================
"""

import os
import sys
import time
import math
import base64
import sqlite3
import argparse
import datetime
import zoneinfo
import requests
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# 1. Environment & Path Setup
# -----------------------------------------------------------------------------
load_dotenv()
load_dotenv(dotenv_path=os.path.expanduser("~/.env"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_LOG_PATH = os.path.join(SCRIPT_DIR, "Strategy_B_Trade_Log.csv")
AUDIT_LOG_PATH = os.path.join(SCRIPT_DIR, "Strategy_B_Scan_Audit.csv")
DB_PATH = os.path.join(SCRIPT_DIR, "trading_brain.db")

API_KEY = os.getenv("TRADING212_API_KEY")
API_SECRET = os.getenv("TRADING212_API_SECRET")
IS_DEMO = os.getenv("TRADING212_DEMO", "true").lower() == "true"

BASE_URL = "https://demo.trading212.com/api/v0" if IS_DEMO else "https://live.trading212.com/api/v0"

# Caches
METADATA_CACHE = {}
YF_TO_T212_MAP = {
    "NVDA": "NVDA_US_EQ",
    "AAPL": "AAPL_US_EQ",
    "MSFT": "MSFT_US_EQ",
    "TSLA": "TSLA_US_EQ",
    "AMZN": "AMZN_US_EQ",
    "GOOGL": "GOOGL_US_EQ",
    "AMD": "AMD_US_EQ",
    "META": "META_US_EQ",
    "SHEL.L": "SHELl_UK_EQ",
    "AZN.L": "AZNl_UK_EQ",
    "HSBA.L": "HSBAl_UK_EQ",
    "ULVR.L": "ULVRl_UK_EQ",
    "BP.L": "BPl_UK_EQ",
    "GSK.L": "GSKl_UK_EQ",
    "BARC.L": "BARCl_UK_EQ",
    "SAP.DE": "SAPd_DE_EQ",
    "SIE.DE": "SIEd_DE_EQ",
    "ALV.DE": "ALVd_DE_EQ",
    "AIR.DE": "AIRd_DE_EQ",
    "DTE.DE": "DTEd_DE_EQ",
    "BMW.DE": "BMWd_DE_EQ",
    "BAS.DE": "BASd_DE_EQ",
    "MC.PA": "MCp_FR_EQ",
    "OR.PA": "ORp_FR_EQ",
    "TTE.PA": "TTEp_FR_EQ",
    "RMS.PA": "RMSp_FR_EQ",
    "ASML.AS": "ASMLa_NL_EQ",
    "INGA.AS": "INGAa_NL_EQ",
    "PRX.AS": "PRXa_NL_EQ",
}

def get_headers():
    credentials = f"{API_KEY}:{API_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json"
    }

# -----------------------------------------------------------------------------
# 2. Database & CSV Logging Systems
# -----------------------------------------------------------------------------
def init_database():
    """Initializes SQLite database for trade tracking and exit management."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Active/Historical Trades Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            ticker TEXT PRIMARY KEY,
            yf_symbol TEXT,
            shares REAL,
            entry_price REAL,
            stop_price REAL,
            target_price REAL,
            opened_at TEXT,
            status TEXT,
            closed_at TEXT,
            exit_price REAL,
            realized_pnl REAL
        )
    """)
    
    # Scan Audit Log Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ticker TEXT,
            price REAL,
            trend_pass INTEGER,
            pullback_pass INTEGER,
            reversal_pass INTEGER,
            decision TEXT,
            reason TEXT
        )
    """)
    conn.commit()
    conn.close()

def init_csv_logs():
    """Ensures CSV files exist with proper headers."""
    if not os.path.exists(TRADE_LOG_PATH):
        with open(TRADE_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Status,Ticker,Category,Date,State,Shares,Entry,Stop,Target,PnL,Return,Fees,Notes\n")

    if not os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Timestamp,Ticker,Price,Trend_Pass,Pullback_Pass,Reversal_Pass,Decision,Reason\n")

def log_scan_audit(ticker: str, price: float, trend_pass: bool, pullback_pass: bool, reversal_pass: bool, decision: str, reason: str):
    """Logs scan results to both SQLite DB and CSV."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Write to CSV
    log_line = f'"{now_str}","{ticker}","{price:.2f}","{trend_pass}","{pullback_pass}","{reversal_pass}","{decision}","{reason}"\n'
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(log_line)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        print(f"[ERROR] Failed writing to CSV audit log: {e}")

    # Write to SQLite DB
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scan_audit (timestamp, ticker, price, trend_pass, pullback_pass, reversal_pass, decision, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (now_str, ticker, price, int(trend_pass), int(pullback_pass), int(reversal_pass), decision, reason))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed writing to DB audit log: {e}")

# -----------------------------------------------------------------------------
# 3. Market Open & Calendar Guard
# -----------------------------------------------------------------------------
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
            print(f"[SKIP] Outside market hours ({market_open.strftime('%H:%M')} - {market_close.strftime('%H:%M')} UTC).")
            return False
    except Exception as e:
        print(f"[ERROR] Market calendar check failed: {e}")
        return True  # Fallback to allow execution if check fails

# -----------------------------------------------------------------------------
# 4. Trading 212 Metadata & API Helpers
# -----------------------------------------------------------------------------
def test_trading212_connection() -> bool:
    """Diagnostic check verifying credentials and permissions."""
    print("\n=======================================================================")
    print("[PRE-FLIGHT] VERIFYING TRADING 212 API CONNECTION")
    print("=======================================================================")
    print(f"[*] Target Environment: {'DEMO' if IS_DEMO else 'LIVE'}")
    print(f"[*] Base URL: {BASE_URL}")

    if not API_KEY:
        print("[CRITICAL] TRADING212_API_KEY environment variable is missing.")
        return False

    try:
        res = requests.get(f"{BASE_URL}/equity/account/summary", headers=get_headers(), timeout=10)
        if res.status_code == 200:
            data = res.json()
            print(" SUCCESS: Connected to Trading 212 API!")
            print(f"   Account ID: {data.get('id')}")
            print(f"   Total Equity: GBP {data.get('totalValue', 0.0):,.2f}")
            print(f"   Free Cash: GBP {data.get('free', 0.0):,.2f}")
            print("=======================================================================\n")
            return True
        else:
            print(f"[FAIL] API Check returned HTTP {res.status_code}: {res.text}")
    except Exception as e:
        print(f"[ERROR] API Connection exception: {e}")

    return False

def load_instrument_metadata():
    """Fetches and caches Trading 212 instrument metadata for valid ticker mapping and precision rules."""
    global METADATA_CACHE
    print("[*] Fetching instrument metadata from Trading 212 API...")
    try:
        res = requests.get(f"{BASE_URL}/equity/metadata/instruments", headers=get_headers(), timeout=15)
        if res.status_code == 200:
            instruments = res.json()
            for inst in instruments:
                ticker = inst.get("ticker")
                if ticker:
                    METADATA_CACHE[ticker] = {
                        "name": inst.get("name"),
                        "minTradeSize": inst.get("minTradeSize", 0.0001),
                        "quantityPrecision": inst.get("quantityPrecision", 4),
                        "currencyCode": inst.get("currencyCode", "USD"),
                        "type": inst.get("type")
                    }
            print(f" SUCCESS: Cached metadata for {len(METADATA_CACHE)} Trading 212 instruments.")
            return True
        else:
            print(f"[WARNING] Metadata fetch returned {res.status_code}: {res.text}")
    except Exception as e:
        print(f"[ERROR] Metadata fetch exception: {e}")
    return False

def resolve_t212_ticker(yf_symbol: str) -> str:
    """Maps Yahoo Finance symbols to exact Trading 212 tickers using metadata."""
    if yf_symbol in YF_TO_T212_MAP:
        mapped = YF_TO_T212_MAP[yf_symbol]
        if mapped in METADATA_CACHE:
            return mapped

    # Dynamic fallback lookup in metadata
    clean_symbol = yf_symbol.replace(".L", "").replace(".DE", "").replace(".PA", "").replace(".AS", "")
    for t212_ticker in METADATA_CACHE:
        if t212_ticker.startswith(clean_symbol + "_"):
            return t212_ticker

    return f"{clean_symbol}_US_EQ"

def fetch_account_summary():
    try:
        res = requests.get(f"{BASE_URL}/equity/account/summary", headers=get_headers(), timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"[ERROR] fetch_account_summary failed: {e}")
    return None

def fetch_open_positions():
    try:
        res = requests.get(f"{BASE_URL}/equity/positions", headers=get_headers(), timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"[ERROR] fetch_open_positions failed: {e}")
    return []

def place_market_order(ticker: str, quantity: float, dry_run: bool = False):
    """Places a market buy/sell order with exact quantity precision."""
    if dry_run:
        print(f"   [DRY-RUN] Would submit Market Order: Ticker={ticker}, Quantity={quantity}")
        return 200, {"dry_run": True, "id": "DRY_RUN_ORDER_123"}

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

# -----------------------------------------------------------------------------
# 5. Position Sizing & Currency Scaling Math
# -----------------------------------------------------------------------------
def get_fx_rate_to_gbp(currency: str) -> float:
    """Returns exchange rate to convert trade currency to GBP."""
    currency = currency.upper()
    if currency == "GBP":
        return 1.0
    
    fx_map = {
        "USD": 0.78,  # $1 USD = £0.78 GBP
        "EUR": 0.85,  # €1 EUR = £0.85 GBP
        "GBX": 0.01   # 1 Pence = £0.01 GBP
    }
    return fx_map.get(currency, 1.0)

def calculate_constrained_position_size(
    account_equity_gbp: float,
    available_cash_gbp: float,
    entry_price: float,
    stop_price: float,
    trade_currency: str,
    quantity_precision: int,
    min_trade_size: float,
    risk_pct: float = 0.01,
    max_cash_pct_per_trade: float = 0.20
) -> float:
    """
    Calculates position size strictly constrained by:
    1. Fixed Fractional Account Risk Budget (1%)
    2. Available Cash Limit (Max 20% of free cash per trade)
    3. Trading 212 Instrument Quantity Precision
    """
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0.0001:
        return 0.0

    fx_to_gbp = get_fx_rate_to_gbp(trade_currency)
    entry_price_gbp = entry_price * fx_to_gbp
    risk_per_share_gbp = risk_per_share * fx_to_gbp

    # 1. Shares based on 1% Risk Limit
    risk_budget_gbp = account_equity_gbp * risk_pct
    shares_by_risk = risk_budget_gbp / risk_per_share_gbp

    # 2. Shares based on Free Cash Constraint
    cash_budget_gbp = available_cash_gbp * max_cash_pct_per_trade
    shares_by_cash = cash_budget_gbp / entry_price_gbp

    # 3. Take conservative minimum
    raw_shares = min(shares_by_risk, shares_by_cash)

    # 4. Truncate quantity to exact API precision rules
    if quantity_precision == 0:
        final_shares = math.floor(raw_shares)
    else:
        multiplier = 10 ** quantity_precision
        final_shares = math.floor(raw_shares * multiplier) / multiplier

    # 5. Final validation checks
    if final_shares < min_trade_size:
        return 0.0

    total_cost_gbp = final_shares * entry_price_gbp
    if total_cost_gbp > available_cash_gbp:
        print(f"   [SIZING REJECT] Total cost (£{total_cost_gbp:.2f}) exceeds free cash (£{available_cash_gbp:.2f})")
        return 0.0

    return final_shares

# -----------------------------------------------------------------------------
# 6. Active Position Exit Lifecycle Manager
# -----------------------------------------------------------------------------
def manage_open_position_exits(dry_run: bool = False):
    """
    Monitors active positions against stop_price and target_price.
    Submits market sell orders if stop or target thresholds are breached.
    """
    print("\n-----------------------------------------------------------------------")
    print("[EXIT MONITOR] CHECKING OPEN POSITIONS FOR STOP-LOSS / TAKE-PROFIT")
    print("-----------------------------------------------------------------------")
    
    t212_positions = fetch_open_positions()
    t212_pos_dict = {p.get("ticker"): p for p in t212_positions}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT ticker, yf_symbol, shares, entry_price, stop_price, target_price FROM trades WHERE status = 'OPEN'")
    db_positions = cursor.fetchall()

    if not db_positions:
        print("[EXIT MONITOR] No active strategy trades being tracked in database.")
        conn.close()
        return

    for pos in db_positions:
        ticker, yf_symbol, shares, entry_price, stop_price, target_price = pos

        # Check if position was closed manually on Trading 212
        if ticker not in t212_pos_dict:
            print(f"[*] {ticker}: Position closed manually on Trading 212 interface.")
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("UPDATE trades SET status = 'CLOSED_MANUAL', closed_at = ? WHERE ticker = ?", (now_str, ticker))
            conn.commit()
            continue

        # Get latest current price
        current_price = float(t212_pos_dict[ticker].get("currentPrice", entry_price))

        print(f"[*] Monitoring {ticker}: Current=${current_price:.2f} | Stop=${stop_price:.2f} | Target=${target_price:.2f}")

        # Check Exit Triggers
        hit_stop = current_price <= stop_price
        hit_target = current_price >= target_price

        if hit_stop or hit_target:
            exit_type = "STOP_LOSS" if hit_stop else "TAKE_PROFIT"
            print(f"--> [EXIT TRIGGER] {exit_type} reached for {ticker}! Submitting Market SELL order...")

            status_code, resp = place_market_order(ticker, -abs(shares), dry_run=dry_run)
            if status_code in (200, 201):
                realized_pnl = (current_price - entry_price) * shares
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                cursor.execute("""
                    UPDATE trades 
                    SET status = ?, closed_at = ?, exit_price = ?, realized_pnl = ?
                    WHERE ticker = ?
                """, (f"CLOSED_{exit_type}", now_str, current_price, realized_pnl, ticker))
                conn.commit()

                # Update CSV log
                log_line = f'"LIVE","{ticker}","US MegaCap","{now_str}","CLOSED","{shares}","{entry_price:.2f}","{stop_price:.2f}","{target_price:.2f}","{realized_pnl:.2f}","0.00","0.00","{exit_type} EXECUTED"\n'
                with open(TRADE_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(log_line)
                    f.flush()

                print(f" SUCCESS: {ticker} position closed. Realized PnL: ${realized_pnl:.2f}")
            else:
                print(f"[ERROR] Failed submitting exit order for {ticker}: {resp}")

    conn.close()

# -----------------------------------------------------------------------------
# 7. Main Scan & Order Execution Loop
# -----------------------------------------------------------------------------
def run_strategy_b_scan(watchlist: list, dry_run: bool = False):
    print("\n=======================================================================")
    print("[BOT] STARTING STRATEGY B SCAN CYCLE")
    print("=======================================================================")
    
    init_database()
    init_csv_logs()

    summary = fetch_account_summary()
    if not summary:
        print("[CRITICAL] Could not connect to Trading 212 API. Aborting scan.")
        return

    account_val_gbp = summary.get("totalValue", 5000.0)
    cash_available_gbp = summary.get("free", 5000.0)
    risk_amount_gbp = account_val_gbp * 0.01

    print(f"[*] Account Equity: GBP {account_val_gbp:,.2f}")
    print(f"[*] Free Cash: GBP {cash_available_gbp:,.2f}")
    print(f"[*] 1% Risk Limit: GBP {risk_amount_gbp:,.2f}")

    # First run exit monitoring on existing positions
    manage_open_position_exits(dry_run=dry_run)

    open_pos = fetch_open_positions()
    active_tickers = [p.get("ticker") for p in open_pos]

    for yf_symbol in watchlist:
        time.sleep(0.3)
        t212_ticker = resolve_t212_ticker(yf_symbol)
        meta = METADATA_CACHE.get(t212_ticker, {
            "minTradeSize": 0.0001,
            "quantityPrecision": 4,
            "currencyCode": "USD"
        })

        if t212_ticker in active_tickers:
            print(f"\n[SKIP] {t212_ticker} ({yf_symbol}): Position already open.")
            log_scan_audit(t212_ticker, 0.0, False, False, False, "NO_INVESTMENT", "Position already open")
            continue

        try:
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period="5d", interval="5m")

            if df.empty or len(df) < 50:
                print(f"\n[SKIP] {yf_symbol}: Insufficient market data.")
                log_scan_audit(t212_ticker, 0.0, False, False, False, "NO_INVESTMENT", "Insufficient market data")
                continue

            # Scale UK stocks from GBX (pence) to GBP (£)
            is_uk_pence = yf_symbol.endswith(".L")
            if is_uk_pence:
                df['Open'] /= 100.0
                df['High'] /= 100.0
                df['Low'] /= 100.0
                df['Close'] /= 100.0

            # Technical Indicators
            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['SMA50'] = df['Close'].rolling(window=50).mean()
            df['TR'] = df[['High', 'Low', 'Close']].apply(
                lambda x: max(x['High'] - x['Low'], abs(x['High'] - df['Close'].shift(1).loc[x.name]), abs(x['Low'] - df['Close'].shift(1).loc[x.name])), 
                axis=1
            )
            df['ATR14'] = df['TR'].rolling(window=14).mean()

            closed_bar = df.iloc[-2]
            recent_bars = df.iloc[-6:-1]

            # Filters
            trend_pass = bool(closed_bar['EMA20'] > closed_bar['SMA50'])
            pullback_pass = bool((recent_bars['Low'] <= (recent_bars['EMA20'] * 1.001)).any())
            reversal_pass = bool(closed_bar['Close'] > closed_bar['Open'])

            current_close = float(closed_bar['Close'])
            current_low = float(closed_bar['Low'])
            ema20 = float(closed_bar['EMA20'])
            sma50 = float(closed_bar['SMA50'])
            atr14 = float(closed_bar['ATR14'])

            currency_symbol = "£" if is_uk_pence or meta.get("currencyCode") == "GBP" else "$"
            print(f"\n[SCAN] Ticker: {t212_ticker} ({yf_symbol})")
            print(f"   Price: {currency_symbol}{current_close:.2f} | 20 EMA: {currency_symbol}{ema20:.2f} | 50 SMA: {currency_symbol}{sma50:.2f} | ATR(14): {currency_symbol}{atr14:.2f}")
            print(f"   Filters -> Trend: {trend_pass} | Pullback: {pullback_pass} | Reversal: {reversal_pass}")

            if trend_pass and pullback_pass and reversal_pass:
                print(f"--> [SIGNAL TRIGGERED] Buy signal on {t212_ticker}!")
                
                entry_price = current_close
                stop_price = current_low - (1.5 * atr14)
                risk_per_share = entry_price - stop_price
                target_price = entry_price + (2.5 * risk_per_share)

                trade_curr = "GBP" if is_uk_pence else meta.get("currencyCode", "USD")
                shares = calculate_constrained_position_size(
                    account_equity_gbp=account_val_gbp,
                    available_cash_gbp=cash_available_gbp,
                    entry_price=entry_price,
                    stop_price=stop_price,
                    trade_currency=trade_curr,
                    quantity_precision=meta.get("quantityPrecision", 4),
                    min_trade_size=meta.get("minTradeSize", 0.0001)
                )

                if shares > 0:
                    print(f"   Calculated Order: BUY {shares} shares of {t212_ticker} @ {currency_symbol}{entry_price:.2f}")
                    
                    status_code, response_data = place_market_order(t212_ticker, shares, dry_run=dry_run)
                    print(f"   [API RESPONSE] Status: {status_code} | Output: {response_data}")

                    if status_code in (200, 201):
                        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Store in SQLite DB for exit tracking
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT OR REPLACE INTO trades 
                            (ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
                        """, (t212_ticker, yf_symbol, shares, entry_price, stop_price, target_price, now_str))
                        conn.commit()
                        conn.close()

                        log_scan_audit(t212_ticker, current_close, trend_pass, pullback_pass, reversal_pass, "INVESTED", "All filters passed - Order placed")
                    else:
                        log_scan_audit(t212_ticker, current_close, trend_pass, pullback_pass, reversal_pass, "ERROR", f"API Error: {response_data}")
                else:
                    print("   [SKIP] Calculated share size failed position constraints / cash limits.")
                    log_scan_audit(t212_ticker, current_close, trend_pass, pullback_pass, reversal_pass, "NO_INVESTMENT", "Share size failed constraints")
            else:
                reasons = []
                if not trend_pass: reasons.append("Failed Trend")
                if not pullback_pass: reasons.append("Failed Pullback")
                if not reversal_pass: reasons.append("Failed Reversal")
                reason_str = " | ".join(reasons)
                print(f"   [NO INVESTMENT] Reason: {reason_str}")
                log_scan_audit(t212_ticker, current_close, trend_pass, pullback_pass, reversal_pass, "NO_INVESTMENT", reason_str)

        except Exception as e:
            print(f"[ERROR] Processing exception for {yf_symbol}: {e}")
            log_scan_audit(t212_ticker, 0.0, False, False, False, "ERROR", f"Exception: {e}")

    print("\n=======================================================================")
    print("[BOT] SCAN CYCLE COMPLETED")
    print("=======================================================================")

# -----------------------------------------------------------------------------
# 8. Entrypoint & Daemon Loop
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Strategy B Automated Trading Engine")
    parser.add_argument("--once", action="store_true", help="Perform a single scan cycle and exit.")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in 5-minute daemon loop.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate signals and orders without API submission.")
    parser.add_argument("--test-connection", action="store_true", help="Test Trading 212 API connection and exit.")
    args = parser.parse_args()

    init_database()
    init_csv_logs()

    if args.test_connection:
        success = test_trading212_connection()
        sys.exit(0 if success else 1)

    if not test_trading212_connection():
        print("[CRITICAL] API connection failed. Stopping execution.")
        sys.exit(1)

    load_instrument_metadata()

    watchlist = [
        "NVDA", "AAPL", "MSFT", "TSLA", "AMZN", "GOOGL", "AMD", "META",
        "SHEL.L", "AZN.L", "HSBA.L", "SAP.DE", "SIE.DE", "MC.PA"
    ]

    if args.daemon:
        print("[DAEMON MODE] Starting continuous 5-minute trading engine...")
        import schedule
        
        def job():
            if is_market_open():
                run_strategy_b_scan(watchlist, dry_run=args.dry_run)
            else:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Market closed. Skipping scan.")

        schedule.every(5).minutes.do(job)
        job()  # Run initial scan immediately

        while True:
            schedule.run_pending()
            time.sleep(10)
    else:
        # Single run mode
        if is_market_open():
            run_strategy_b_scan(watchlist, dry_run=args.dry_run)
        else:
            print("[STANDBY] Market is closed. Skipping scan.")
        sys.exit(0)

if __name__ == "__main__":
    main()
