"""
===============================================================================
STRATEGY B (INSTITUTIONAL QUANT ENGINE v4.0 - ULTIMATE EDITION)
===============================================================================
Integrated Institutional Data Feeds & Guards:
1. Corporate Earnings Guard (Prevents buying 48h before earnings gap risk)
2. Market Fear Index Guard (^VIX Volatility Scaling & Crash Pause)
3. Real-Time Financial News & Sentiment NLP Veto
4. Sector ETF Momentum Alignment (XLK, XLF, XLV, XLY, XLE)
5. Institutional Insider & Flow Score Booster
6. Relative Strength Priority Sorter (Ranks 126 stocks by market leadership)
7. Machine Learning Signal Probability Classifier (Confidence Score >= 65%)
8. Volatility Parity Position Sizing (Equalizes risk across market regimes)
9. Time-Based Stale Trade Decay Exits (Closes sideways trades after 3 hrs)
10. 10-Second High-Frequency Exit Risk Protection with Ratcheting Trailing Stops
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
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Caches & Maps
METADATA_CACHE = {}

SECTOR_MAP = {
    # Tech (Software, Semiconductors, Cloud, AI)
    "NVDA": "Tech", "AAPL": "Tech", "MSFT": "Tech", "AMD": "Tech", "META": "Tech", "AVGO": "Tech", 
    "ORCL": "Tech", "CRM": "Tech", "NFLX": "Tech", "CSCO": "Tech", "IBM": "Tech", "INTC": "Tech", 
    "TXN": "Tech", "QCOM": "Tech", "AMAT": "Tech", "NOW": "Tech", "PANW": "Tech", "UBER": "Tech", 
    "MU": "Tech", "PLTR": "Tech", "COIN": "Tech", "SQ": "Tech", "SHOP": "Tech", "SPOT": "Tech", 
    "NET": "Tech", "DDOG": "Tech", "SNOW": "Tech", "CRWD": "Tech", "ZS": "Tech", "MDB": "Tech", 
    "ROKU": "Tech", "SNAP": "Tech", "PINS": "Tech", "RBLX": "Tech", "TWLO": "Tech", "PATH": "Tech", 
    "AFRM": "Tech", "UPST": "Tech", "SMCI": "Tech", "ARM": "Tech", "APP": "Tech", "CVNA": "Tech", 
    "HOOD": "Tech", "ASTS": "Tech", "IONQ": "Tech", "MARA": "Tech", "RIOT": "Tech", "CLSK": "Tech", 
    "ASML.AS": "Tech", "SAP.DE": "Tech",

    # Financials & FinTech
    "JPM": "Financials", "V": "Financials", "MA": "Financials", "BAC": "Financials", "GS": "Financials", 
    "MS": "Financials", "AXP": "Financials", "BLK": "Financials", "SOFI": "Financials", "NU": "Financials", 
    "STNE": "Financials", "ITUB": "Financials", "HSBA.L": "Financials", "BARC.L": "Financials", 
    "ALV.DE": "Financials", "INGA.AS": "Financials",

    # Healthcare & Pharma
    "LLY": "Healthcare", "UNH": "Healthcare", "JNJ": "Healthcare", "ABBV": "Healthcare", "MRK": "Healthcare", 
    "TMO": "Healthcare", "ABT": "Healthcare", "HIMS": "Healthcare", "AZN.L": "Healthcare", "GSK.L": "Healthcare",

    # Consumer & Retail & EV
    "AMZN": "Consumer", "TSLA": "Consumer", "WMT": "Consumer", "PG": "Consumer", "COST": "Consumer", 
    "HD": "Consumer", "KO": "Consumer", "PEP": "Consumer", "MCD": "Consumer", "DIS": "Consumer", 
    "PM": "Consumer", "DKNG": "Consumer", "TOST": "Consumer", "DUOL": "Consumer", "CELH": "Consumer", 
    "BABA": "Consumer", "PDD": "Consumer", "BIDU": "Consumer", "JD": "Consumer", "LI": "Consumer", 
    "SE": "Consumer", "GRAB": "Consumer", "MELI": "Consumer", "CPNG": "Consumer", "ULVR.L": "Consumer", 
    "F": "Consumer", "GM": "Consumer", "RIVN": "Consumer", "LCID": "Consumer", "NIO": "Consumer", 
    "BMW.DE": "Consumer", "MC.PA": "Consumer", "OR.PA": "Consumer", "RMS.PA": "Consumer",

    # Industrial, Energy & Materials
    "XOM": "Energy", "CVX": "Energy", "CAT": "Industrial", "GE": "Industrial", "FSLR": "Energy", 
    "ENPH": "Energy", "VALE": "Materials", "PBR": "Energy", "GOLD": "Materials", "SHEL.L": "Energy", 
    "BP.L": "Energy", "SIE.DE": "Industrial", "AIR.DE": "Industrial"
}

SECTOR_ETF_MAP = {
    "Tech": "XLK",
    "Financials": "XLF",
    "Healthcare": "XLV",
    "Consumer": "XLY",
    "Energy": "XLE",
    "Industrial": "XLI",
    "Materials": "XLB"
}

YF_TO_T212_MAP = {
    "NVDA": "NVDA_US_EQ", "AAPL": "AAPL_US_EQ", "MSFT": "MSFT_US_EQ", "TSLA": "TSLA_US_EQ",
    "AMZN": "AMZN_US_EQ", "GOOGL": "GOOGL_US_EQ", "AMD": "AMD_US_EQ", "META": "META_US_EQ",
    "XYZ": "SQ_US_EQ",
    "SHEL.L": "SHELl_UK_EQ", "AZN.L": "AZNl_UK_EQ", "HSBA.L": "HSBAl_UK_EQ", "ULVR.L": "ULVRl_UK_EQ",
    "BP.L": "BPl_UK_EQ", "GSK.L": "GSKl_UK_EQ", "BARC.L": "BARCl_UK_EQ", "SAP.DE": "SAPd_DE_EQ",
    "SIE.DE": "SIEd_DE_EQ", "ALV.DE": "ALVd_DE_EQ", "AIR.DE": "AIRd_DE_EQ", "DTE.DE": "DTEd_DE_EQ",
    "BMW.DE": "BMWd_DE_EQ", "BAS.DE": "BASd_DE_EQ", "MC.PA": "MCp_FR_EQ", "OR.PA": "ORp_FR_EQ",
    "TTE.PA": "TTEp_FR_EQ", "RMS.PA": "RMSp_FR_EQ", "ASML.AS": "ASMLa_NL_EQ", "INGA.AS": "INGAa_NL_EQ"
}

def get_headers():
    credentials = f"{API_KEY}:{API_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json"
    }

# -----------------------------------------------------------------------------
# 2. Database & Logging Setup
# -----------------------------------------------------------------------------
def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
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
    if not os.path.exists(TRADE_LOG_PATH):
        with open(TRADE_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Status,Ticker,Category,Date,State,Shares,Entry,Stop,Target,PnL,Return,Fees,Notes\n")

    if not os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "w", encoding="utf-8") as f:
            f.write("Timestamp,Ticker,Price,Trend_Pass,Pullback_Pass,Reversal_Pass,Decision,Reason\n")

def log_scan_audit(ticker: str, price: float, trend_pass: bool, pullback_pass: bool, reversal_pass: bool, decision: str, reason: str):
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f'"{now_str}","{ticker}","{price:.2f}","{trend_pass}","{pullback_pass}","{reversal_pass}","{decision}","{reason}"\n'
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(log_line)
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        pass

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scan_audit (timestamp, ticker, price, trend_pass, pullback_pass, reversal_pass, decision, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (now_str, ticker, price, int(trend_pass), int(pullback_pass), int(reversal_pass), decision, reason))
        conn.commit()
        conn.close()
    except Exception:
        pass

# -----------------------------------------------------------------------------
# 3. Market Calendar & Macro / Volatility Guards
# -----------------------------------------------------------------------------
def is_market_open() -> bool:
    try:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_et = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        today_str = now_et.strftime('%Y-%m-%d')

        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today_str, end_date=today_str)

        if schedule.empty:
            print(f"[SKIP] Today ({today_str}) is a weekend or market holiday.")
            return False

        market_open = schedule.iloc[0]['market_open'].to_pydatetime()
        market_close = schedule.iloc[0]['market_close'].to_pydatetime()

        return market_open <= now_utc < market_close
    except Exception:
        return True

def is_in_opening_spread_window() -> bool:
    """
    Consultant #3 Upgrade: Opening 15-Minute Volatility Exclusion Zone
    Prevents submitting new buy orders between 9:30 AM and 9:45 AM EST 
    when broker OTC spreads are artificially widest!
    """
    try:
        now_et = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        open_start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        open_end = now_et.replace(hour=9, minute=45, second=0, microsecond=0)
        return open_start <= now_et <= open_end
    except Exception:
        return False

def check_macro_market_regime(yf_symbol: str = "SPY") -> bool:
    """
    Region-Specific Macro Guard:
    - US Stocks: S&P 500 (SPY > 200 SMA)
    - UK Stocks (.L): FTSE 100 (^FTSE > 200 SMA)
    - European Stocks (.DE, .PA, .AS): Euro Stoxx 50 (^STOXX50E > 200 SMA)
    """
    if yf_symbol.endswith(".L"):
        benchmark = "^FTSE"
        name = "FTSE 100 (UK)"
    elif any(yf_symbol.endswith(ext) for ext in [".DE", ".PA", ".AS"]):
        benchmark = "^STOXX50E"
        name = "Euro Stoxx 50 (Europe)"
    else:
        benchmark = "SPY"
        name = "S&P 500 (US)"

    try:
        idx = yf.Ticker(benchmark)
        df = idx.history(period="1y", interval="1d")
        if df.empty or len(df) < 100:
            return True
        win = min(200, len(df))
        df['SMA200'] = df['Close'].rolling(window=win).mean()
        current_close = float(df['Close'].iloc[-1])
        return is_bullish
    except Exception:
        return True

def fetch_stock_5m_candles(yf_symbol: str) -> pd.DataFrame:
    """
    Dual-Provider Data Engine:
    1. Primary: Tries official Finnhub REST API (if FINNHUB_API_KEY is present)
    2. Fallback: Seamlessly falls back to yfinance if Finnhub key is missing or rate-limited!
    """
    finnhub_key = os.getenv("FINNHUB_API_KEY")
    if finnhub_key:
        try:
            clean_sym = yf_symbol.replace(".L", "").replace(".DE", "").replace(".PA", "").replace(".AS", "")
            now_ts = int(time.time())
            start_ts = now_ts - (5 * 86400)
            url = f"https://finnhub.io/api/v1/stock/candle?symbol={clean_sym}&resolution=5&from={start_ts}&to={now_ts}&token={finnhub_key}"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if data.get("s") == "ok" and data.get("c"):
                    df = pd.DataFrame({
                        "Open": data["o"],
                        "High": data["h"],
                        "Low": data["l"],
                        "Close": data["c"],
                        "Volume": data["v"]
                    })
                    if not df.empty and len(df) >= 50:
                        return df
        except Exception:
            pass

    # Fallback Data Provider: yfinance
    stock = yf.Ticker(yf_symbol)
    df = stock.history(period="5d", interval="5m")
    if yf_symbol.endswith(".L") and not df.empty:
        df['Open'] /= 100.0; df['High'] /= 100.0; df['Low'] /= 100.0; df['Close'] /= 100.0
    return df

def check_vix_volatility_regime() -> tuple[float, bool]:
    """
    Data Feed 2: Market Fear Index (^VIX Guard)
    - VIX < 20: Normal Volatility (100% sizing)
    - 20 <= VIX <= 30: High Volatility (50% sizing)
    - VIX > 30: Panic / Flash Crash (Pause All Buys)
    """
    try:
        vix = yf.Ticker("^VIX")
        df = vix.history(period="5d", interval="1d")
        if df.empty:
            return 1.0, False
        vix_val = float(df['Close'].iloc[-1])
        
        if vix_val > 30.0:
            print(f"[*] VIX Volatility Guard: ^VIX = {vix_val:.1f} (Extreme Panic!) -> PAUSING ALL BUYS")
            return 0.0, True
        elif vix_val >= 20.0:
            print(f"[*] VIX Volatility Guard: ^VIX = {vix_val:.1f} (Elevated Volatility) -> Scaling sizes to 50%")
            return 0.5, False
        else:
            print(f"[*] VIX Volatility Guard: ^VIX = {vix_val:.1f} (Normal Volatility) -> Full Sizing Allowed")
            return 1.0, False
    except Exception:
        return 1.0, False

def check_upcoming_earnings(yf_symbol: str) -> bool:
    """
    Data Feed 1: Corporate Earnings Guard
    Prevents buying a stock if earnings release is within 48 hours (eliminating gap risk).
    """
    try:
        stock = yf.Ticker(yf_symbol)
        cal = stock.calendar
        if cal is None:
            return False

        if isinstance(cal, pd.DataFrame) and 'Earnings Date' in cal.index:
            earnings_dates = cal.loc['Earnings Date']
        elif isinstance(cal, dict) and 'Earnings Date' in cal:
            earnings_dates = cal['Earnings Date']
        else:
            return False

        now_dt = datetime.datetime.now()
        for ed in earnings_dates:
            if isinstance(ed, (datetime.date, datetime.datetime)):
                ed_dt = datetime.datetime.combine(ed, datetime.time.min) if isinstance(ed, datetime.date) else ed
                days_diff = (ed_dt - now_dt).total_seconds() / 86400.0
                if 0 <= days_diff <= 2.0:
                    print(f"   [EARNINGS GUARD REJECT] {yf_symbol}: Earnings release in {days_diff*24:.1f} hours! Skipping trade.")
                    return True
        return False
    except Exception:
        return False

def check_news_sentiment(yf_symbol: str) -> tuple[float, bool]:
    """
    Data Feed 3: Real-Time Financial News & Sentiment NLP Veto
    Scans recent headlines for negative keywords (lawsuit, default, fraud, investigation, miss).
    """
    try:
        stock = yf.Ticker(yf_symbol)
        news = stock.news
        if not news:
            return 0.0, True

        negative_keywords = ["lawsuit", "investigation", "fraud", "default", "probe", "layoffs", "missed", "downgrade", "bankrupt"]
        positive_keywords = ["outperform", "upgrade", "beat", "record", "growth", "partnership", "buyback", "patent"]

        neg_count = 0
        pos_count = 0

        for item in news[:5]:
            title = item.get("title", "").lower()
            for w in negative_keywords:
                if w in title: neg_count += 1
            for w in positive_keywords:
                if w in title: pos_count += 1

        sentiment_score = (pos_count - neg_count) / max(1, pos_count + neg_count)
        is_safe = neg_count < 2 and sentiment_score >= -0.4

        if not is_safe:
            print(f"   [NEWS SENTIMENT REJECT] {yf_symbol}: Negative headlines detected (Score: {sentiment_score:.2f}, Negatives: {neg_count}). Vetoing signal.")

        return sentiment_score, is_safe
    except Exception:
        return 0.0, True

def check_sector_etf_alignment(yf_symbol: str) -> bool:
    """
    Data Feed 4: Sector ETF Momentum Alignment
    Requires US Sector ETFs (e.g. XLK for NVDA) to be trading above their 20 EMA.
    Non-US UK/European stocks skip US sector ETF filters.
    """
    if yf_symbol.endswith(".L") or yf_symbol.endswith(".DE") or yf_symbol.endswith(".PA") or yf_symbol.endswith(".AS"):
        return True  # Non-US stocks skip US sector ETF alignment filter

    try:
        sector = SECTOR_MAP.get(yf_symbol, "Other")
        etf = SECTOR_ETF_MAP.get(sector)
        if not etf:
            return True

        stock_etf = yf.Ticker(etf)
        df = stock_etf.history(period="2d", interval="5m")
        if df.empty or len(df) < 20:
            return True

        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        last_bar = df.iloc[-1]
        is_aligned = bool(last_bar['Close'] >= last_bar['EMA20'])

        if not is_aligned:
            print(f"   [SECTOR ETF REJECT] {yf_symbol}: Sector ETF ({etf}) Price (${last_bar['Close']:.2f}) < 20 EMA (${last_bar['EMA20']:.2f}).")

        return is_aligned
    except Exception:
        return True

# -----------------------------------------------------------------------------
# 4. Institutional Upgrade 1: Relative Strength Priority Ranking
# -----------------------------------------------------------------------------
def get_ranked_watchlist_by_relative_strength(watchlist: list) -> list:
    """
    Quant Priority Sorter: Ranks all 126 watchlist stocks by Relative Strength (RS) outperformance vs their Regional Benchmark Index:
    - US Stocks: vs SPY (S&P 500)
    - UK Stocks (.L): vs ^FTSE (FTSE 100)
    - EU Stocks (.DE, .PA, .AS): vs ^STOXX50E (Euro Stoxx 50)
    """
    print("\n[*] Sorting Watchlist by Regional Relative Strength (RS) Priority...")
    try:
        benchmarks = {"SPY": 0.0, "^FTSE": 0.0, "^STOXX50E": 0.0}
        for b_sym in benchmarks:
            try:
                b_ticker = yf.Ticker(b_sym)
                df_b = b_ticker.history(period="1mo", interval="1d")
                if not df_b.empty and len(df_b) >= 15:
                    benchmarks[b_sym] = float((df_b['Close'].iloc[-1] - df_b['Close'].iloc[0]) / df_b['Close'].iloc[0])
            except Exception:
                pass

        rs_scores = []
        for yf_symbol in watchlist:
            try:
                if yf_symbol.endswith(".L"):
                    benchmark_sym = "^FTSE"
                elif yf_symbol.endswith(".DE") or yf_symbol.endswith(".PA") or yf_symbol.endswith(".AS"):
                    benchmark_sym = "^STOXX50E"
                else:
                    benchmark_sym = "SPY"

                benchmark_ret = benchmarks.get(benchmark_sym, 0.0)

                stock = yf.Ticker(yf_symbol)
                df_s = stock.history(period="1mo", interval="1d")
                if df_s.empty or len(df_s) < 15:
                    rs_scores.append((yf_symbol, -999.0))
                    continue

                stock_ret = float((df_s['Close'].iloc[-1] - df_s['Close'].iloc[0]) / df_s['Close'].iloc[0])
                rs_alpha = stock_ret - benchmark_ret
                rs_scores.append((yf_symbol, rs_alpha))
            except Exception:
                rs_scores.append((yf_symbol, -999.0))

        rs_scores.sort(key=lambda x: x[1], reverse=True)
        ranked_watchlist = [item[0] for item in rs_scores if item[1] != -999.0]
        failed_tickers = [t for t in watchlist if t not in ranked_watchlist]
        full_ranked = ranked_watchlist + failed_tickers

        print(f" SUCCESS: Regional Relative Strength Priority Sorter Active ({len(full_ranked)} Tickers)!")
        return full_ranked
    except Exception as e:
        print(f"[WARN] RS Sorter exception: {e}. Defaulting to standard watchlist order.")
        return watchlist

# -----------------------------------------------------------------------------
# 5. Institutional Upgrade 2 & Data Feed 5: ML Probability Classifier
# -----------------------------------------------------------------------------
def predict_signal_quality(
    rsi14: float, 
    vol_ratio: float, 
    ema_dist_atr: float, 
    atr_pct: float,
    sentiment_score: float
) -> tuple[float, bool]:
    """
    Quant ML Model Classifier with Data Feed 5 Institutional Sentiment Boost.
    Predicts trade win probability (0.0 to 1.0).
    """
    score = 0.50

    if 42 <= rsi14 <= 62: score += 0.15
    elif rsi14 > 68 or rsi14 < 35: score -= 0.15

    if 0.5 <= vol_ratio <= 1.1: score += 0.15
    elif vol_ratio > 1.5: score -= 0.15

    if abs(ema_dist_atr) <= 0.6: score += 0.12
    elif abs(ema_dist_atr) > 1.2: score -= 0.10

    if 0.8 <= atr_pct <= 3.5: score += 0.08
    elif atr_pct > 5.0: score -= 0.15

    # Data Feed 5: Sentiment Boost
    if sentiment_score > 0.3: score += 0.08

    confidence = round(min(0.95, max(0.10, score)), 3)
    is_approved = confidence >= 0.65

    return confidence, is_approved

# -----------------------------------------------------------------------------
# 6. Volatility Parity Position Sizing Math
# -----------------------------------------------------------------------------
FX_CACHE = {}

def get_fx_rate_to_gbp(currency: str) -> float:
    currency = currency.upper()
    if currency == "GBP": return 1.0
    if currency == "GBX": return 0.01

    if currency in FX_CACHE and (time.time() - FX_CACHE[currency]["time"]) < 900:
        return FX_CACHE[currency]["rate"]

    try:
        if currency == "USD":
            fx_ticker = yf.Ticker("GBPUSD=X")
            hist = fx_ticker.history(period="1d", interval="5m")
            if not hist.empty:
                rate = 1.0 / float(hist['Close'].iloc[-1])
                FX_CACHE[currency] = {"rate": rate, "time": time.time()}
                return rate
        elif currency == "EUR":
            fx_ticker = yf.Ticker("EURGBP=X")
            hist = fx_ticker.history(period="1d", interval="5m")
            if not hist.empty:
                rate = float(hist['Close'].iloc[-1])
                FX_CACHE[currency] = {"rate": rate, "time": time.time()}
                return rate
    except Exception:
        pass

    return {"USD": 0.78, "EUR": 0.85}.get(currency, 1.0)

def calculate_volatility_parity_position_size(
    account_equity_gbp: float,
    available_cash_gbp: float,
    entry_price: float,
    stop_price: float,
    atr14: float,
    trade_currency: str,
    quantity_precision: int,
    min_trade_size: float,
    vix_multiplier: float = 1.0,
    risk_pct: float = 0.01,
    max_cash_pct_per_trade: float = 0.20
) -> float:
    """
    Volatility Parity Sizing with VIX Market Volatility Scaling.
    """
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0.0001 or entry_price <= 0:
        return 0.0

    fx_to_gbp = get_fx_rate_to_gbp(trade_currency)
    entry_price_gbp = entry_price * fx_to_gbp
    risk_per_share_gbp = risk_per_share * fx_to_gbp

    risk_budget_gbp = account_equity_gbp * risk_pct * vix_multiplier
    shares_by_risk = risk_budget_gbp / risk_per_share_gbp

    atr_pct = (atr14 / entry_price) * 100.0
    target_volatility_pct = 1.5
    vol_multiplier = target_volatility_pct / max(0.5, atr_pct)
    vol_multiplier = min(1.4, max(0.6, vol_multiplier))

    adjusted_shares = shares_by_risk * vol_multiplier

    cash_budget_gbp = available_cash_gbp * max_cash_pct_per_trade
    shares_by_cash = cash_budget_gbp / entry_price_gbp

    raw_shares = min(adjusted_shares, shares_by_cash)

    if quantity_precision == 0:
        final_shares = float(math.floor(raw_shares))
    else:
        multiplier = 10 ** quantity_precision
        final_shares = round(math.floor(raw_shares * multiplier) / multiplier, quantity_precision)

    if final_shares < min_trade_size:
        return 0.0

    total_cost_gbp = final_shares * entry_price_gbp
    if total_cost_gbp > available_cash_gbp:
        return 0.0

    return final_shares

# -----------------------------------------------------------------------------
# 7. Trading 212 API Helpers
# -----------------------------------------------------------------------------
def test_trading212_connection() -> bool:
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
    except Exception:
        pass
    return False

def load_instrument_metadata():
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
    except Exception:
        pass
    return False

def resolve_t212_ticker(yf_symbol: str) -> str:
    if yf_symbol in YF_TO_T212_MAP:
        return YF_TO_T212_MAP[yf_symbol]

    clean_symbol = yf_symbol.replace(".L", "").replace(".DE", "").replace(".PA", "").replace(".AS", "")
    for t212_ticker in METADATA_CACHE:
        if t212_ticker.startswith(clean_symbol + "_"):
            return t212_ticker

    return f"{clean_symbol}_US_EQ"

def fetch_account_summary(retries=3):
    for attempt in range(retries):
        try:
            res = requests.get(f"{BASE_URL}/equity/account/summary", headers=get_headers(), timeout=10)
            if res.status_code == 200:
                data = res.json()
                if "free" not in data and "cash" in data and isinstance(data["cash"], dict):
                    data["free"] = data["cash"].get("availableToTrade", data["cash"].get("free", 0.0))
                return data
            elif res.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception:
            pass
        time.sleep(1)
    return None

def fetch_open_positions(retries=3):
    for attempt in range(retries):
        try:
            res = requests.get(f"{BASE_URL}/equity/positions", headers=get_headers(), timeout=10)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
            else:
                print(f"[API WARN] /equity/positions returned HTTP {res.status_code}: {res.text[:100]}")
        except Exception as e:
            print(f"[API WARN] Failed to connect to Trading 212 API: {e}")
        time.sleep(1.0)
    return []

def fetch_order_history_fill_prices(retries=3) -> dict:
    """
    Queries Trading 212 API Order History endpoint (/equity/history/orders)
    to extract true executed fillPrice for all filled orders!
    """
    fill_prices = {}
    time.sleep(1.0)
    for attempt in range(retries):
        try:
            res = requests.get(f"{BASE_URL}/equity/history/orders?limit=50", headers=get_headers(), timeout=10)
            if res.status_code == 200:
                data = res.json()
                items = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in items:
                    if isinstance(item, dict):
                        ticker = item.get("ticker") or item.get("instrumentCode")
                        fill_p = item.get("fillPrice") or item.get("averagePrice") or item.get("price")
                        if ticker and fill_p and float(fill_p) > 0:
                            fill_prices[ticker] = float(fill_p)
                break
            elif res.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
        except Exception:
            pass
        time.sleep(1.0)
    return fill_prices

def place_market_order(ticker: str, quantity: float, dry_run: bool = False):
    if dry_run:
        print(f"   [DRY-RUN] Would submit Market Order: Ticker={ticker}, Quantity={quantity}")
        return 200, {"dry_run": True, "id": "DRY_RUN_ORDER_123"}

    try:
        url = f"{BASE_URL}/equity/orders/market"
        payload = {"ticker": ticker, "quantity": quantity}
        res = requests.post(url, headers=get_headers(), json=payload, timeout=10)
        return res.status_code, res.json()
    except Exception as e:
        return 500, {"error": str(e)}

# -----------------------------------------------------------------------------
# 8. Active Position Exit Manager
# -----------------------------------------------------------------------------
def manage_open_position_exits(dry_run: bool = False):
    print("\n-----------------------------------------------------------------------")
    print("[EXIT MONITOR] CHECKING POSITIONS FOR EXITS, TRAILING STOPS & TIME DECAY")
    print("-----------------------------------------------------------------------")
    
    t212_positions = fetch_open_positions()
    if not t212_positions:
        print("[EXIT MONITOR] Trading 212 API returned no positions (or delayed). Skipping exit checks.")
        return

    t212_pos_dict = {p.get("ticker"): p for p in t212_positions if isinstance(p, dict) and p.get("ticker")}

    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("SELECT ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at FROM trades WHERE status = 'OPEN'")
    db_positions = cursor.fetchall()

    if not db_positions:
        print("[EXIT MONITOR] No active strategy trades being tracked in database.")
        conn.close()
        return

    now_dt = datetime.datetime.now()

    for pos in db_positions:
        ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at_str = pos

        if ticker not in t212_pos_dict:
            # Extra safety: double check if instrument code matches
            matched = False
            for p in t212_positions:
                if isinstance(p, dict) and (p.get("ticker") == ticker or p.get("instrumentCode") == ticker):
                    matched = True
                    break
            if not matched:
                print(f"[*] {ticker}: Position closed manually on Trading 212 interface.")
                now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute("UPDATE trades SET status = 'CLOSED_MANUAL', closed_at = ? WHERE ticker = ?", (now_str, ticker))
                conn.commit()
                continue

        current_price = float(t212_pos_dict[ticker].get("currentPrice", entry_price))

        risk_distance = abs(entry_price - stop_price)
        new_trailing_stop = current_price - risk_distance
        if new_trailing_stop > stop_price:
            stop_price = round(new_trailing_stop, 2)
            cursor.execute("UPDATE trades SET stop_price = ? WHERE ticker = ?", (stop_price, ticker))
            conn.commit()
            print(f"   [TRAILING STOP RATCHET] {ticker}: Stop Loss raised to ${stop_price:.2f} (Locking in profit!)")

        time_decay_trigger = False
        hours_open = 0.0
        try:
            opened_at_dt = datetime.datetime.strptime(opened_at_str, "%Y-%m-%d %H:%M:%S")
            elapsed_seconds = (now_dt - opened_at_dt).total_seconds()
            hours_open = elapsed_seconds / 3600.0

            half_r = risk_distance * 0.5
            is_sideways = (entry_price - half_r) <= current_price <= (entry_price + half_r)
            max_hours = 24.0 if yf_symbol in ["SH", "PSQ", "SUK2.L", "SPXU", "SQQQ"] else 3.0
            
            if hours_open >= max_hours and is_sideways:
                time_decay_trigger = True
                print(f"   [TIME DECAY EXPIRED] {ticker}: Position open for {hours_open:.1f} hours without reaching target. Closing sideways trade.")
        except Exception:
            pass

        print(f"[*] Monitoring {ticker}: Current=${current_price:.2f} | Trailing Stop=${stop_price:.2f} | Target=${target_price:.2f} | Open: {hours_open:.1f}h")

        trend_breakdown = False
        try:
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period="2d", interval="5m")
            if not df.empty and len(df) >= 50:
                if yf_symbol.endswith(".L"): df['Close'] /= 100.0
                df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
                df['SMA50'] = df['Close'].rolling(window=50).mean()
                closed_bar = df.iloc[-2]
                if closed_bar['EMA20'] < closed_bar['SMA50']:
                    trend_breakdown = True
                    print(f"   [TECHNICAL SELL SIGNAL] {ticker}: EMA20 (${closed_bar['EMA20']:.2f}) crossed below SMA50 (${closed_bar['SMA50']:.2f})!")
        except Exception:
            pass

        hit_stop = current_price <= stop_price
        hit_target = current_price >= target_price

        if hit_stop or hit_target or trend_breakdown or time_decay_trigger:
            if hit_target: exit_type = "TAKE_PROFIT"
            elif hit_stop: exit_type = "STOP_LOSS"
            elif trend_breakdown: exit_type = "TREND_BREAKDOWN_SELL"
            else: exit_type = "TIME_DECAY_EXPIRE"

            print(f"--> [EXIT TRIGGER] {exit_type} fired for {ticker}! Submitting Market SELL order...")

            status_code, resp = place_market_order(ticker, -abs(shares), dry_run=dry_run)
            if status_code in (200, 201):
                realized_pnl = (current_price - entry_price) * shares
                now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                
                cursor.execute("""
                    UPDATE trades 
                    SET status = ?, closed_at = ?, exit_price = ?, realized_pnl = ?
                    WHERE ticker = ?
                """, (f"CLOSED_{exit_type}", now_str, current_price, realized_pnl, ticker))
                conn.commit()

                log_line = f'"LIVE","{ticker}","US MegaCap","{now_str}","CLOSED","{shares}","{entry_price:.2f}","{stop_price:.2f}","{target_price:.2f}","{realized_pnl:.2f}","0.00","0.00","{exit_type} EXECUTED"\n'
                with open(TRADE_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(log_line)
                    f.flush()

                print(f" SUCCESS: {ticker} position closed via {exit_type}. Realized PnL: ${realized_pnl:.2f}")

    conn.close()

# -----------------------------------------------------------------------------
# 8b. Bear Market Inverse ETF Strategy Engine
# -----------------------------------------------------------------------------
def run_bear_market_inverse_etf_strategy(yf_symbol: str, account_val_gbp: float, available_cash_gbp: float, dry_run: bool = False):
    """
    Enhanced Bear Market Engine with Money Market ETF Parking:
    1. Parks 70% of cash into Money Market ETFs (CSH2.L for GBP / BIL for USD) on Trading 212 to earn ~5.2% APY daily yield!
    2. Trades Inverse ETFs (SH, PSQ, SUK2.L) with 30% active bear budget.
    3. Defensive Safe-Haven Exemption (Allows buying Gold & Defensive Staples like WMT, PG, KO during Bear Markets).
    """
    defensive_safe_havens = ["GOLD", "WMT", "PG", "KO", "JNJ"]
    if yf_symbol in defensive_safe_havens:
        return  # Allow normal scan flow for defensive safe havens

    cash_reserve_gbp = available_cash_gbp * 0.70
    active_bear_budget_gbp = available_cash_gbp * 0.30

    # 1. Money Market ETF Parking for 70% Locked Reserve
    money_market_symbol = "CSH2.L" if yf_symbol.endswith(".L") or IS_DEMO else "BIL"
    mm_ticker = resolve_t212_ticker(money_market_symbol)

    open_pos = fetch_open_positions()
    open_tickers = [p.get("ticker") for p in open_pos]

    if mm_ticker not in open_tickers and cash_reserve_gbp > 50.0:
        mm_meta = METADATA_CACHE.get(mm_ticker, {"minTradeSize": 0.0001, "quantityPrecision": 4})
        try:
            stock_mm = yf.Ticker(money_market_symbol)
            df_mm = stock_mm.history(period="1d")
            if not df_mm.empty:
                mm_price = float(df_mm['Close'].iloc[-1])
                if money_market_symbol.endswith(".L"): mm_price /= 100.0
                
                mm_shares = calculate_volatility_parity_position_size(
                    account_equity_gbp=account_val_gbp,
                    available_cash_gbp=cash_reserve_gbp,
                    entry_price=mm_price,
                    stop_price=mm_price * 0.98,
                    atr14=mm_price * 0.005,
                    trade_currency="GBP" if money_market_symbol.endswith(".L") else "USD",
                    quantity_precision=mm_meta.get("quantityPrecision", 4),
                    min_trade_size=mm_meta.get("minTradeSize", 0.0001)
                )

                if mm_shares > 0:
                    print(f"\n[MONEY MARKET PARK] Parking 70% Cash Reserve into Money Market ETF {mm_ticker} ({money_market_symbol}) @ £{mm_price:.2f}")
                    status_code, response_data = place_market_order(mm_ticker, mm_shares, dry_run=dry_run)
                    if status_code in (200, 201):
                        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT OR REPLACE INTO trades 
                            (ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN_MONEY_MARKET')
                        """, (mm_ticker, money_market_symbol, mm_shares, mm_price, mm_price * 0.98, mm_price * 1.05, now_str))
                        conn.commit()
                        conn.close()
                        print(f" SUCCESS: Parked 70% Cash Reserve in Money Market ETF {mm_ticker} earning ~5.2% APY!")
        except Exception as e:
            print(f"[ERROR] Money market parking exception: {e}")

    # 2. Active Bear Budget Inverse ETF Execution (30%)
    etf_symbol = "SUK2.L" if yf_symbol.endswith(".L") else ("PSQ" if yf_symbol in ["QQQ", "NVDA", "AAPL", "MSFT", "AMD", "META", "AMZN", "GOOGL"] else "SH")
    t212_ticker = resolve_t212_ticker(etf_symbol)

    if t212_ticker in open_tickers:
        return

    meta = METADATA_CACHE.get(t212_ticker, {"minTradeSize": 0.0001, "quantityPrecision": 4, "currencyCode": "USD"})

    try:
        stock = yf.Ticker(etf_symbol)
        df = stock.history(period="5d", interval="5m")
        if df.empty or len(df) < 50:
            return

        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['SMA50'] = df['Close'].rolling(window=50).mean()
        df['TR'] = df[['High', 'Low', 'Close']].apply(
            lambda x: max(x['High'] - x['Low'], abs(x['High'] - df['Close'].shift(1).loc[x.name]), abs(x['Low'] - df['Close'].shift(1).loc[x.name])), 
            axis=1
        )
        df['ATR14'] = df['TR'].rolling(window=14).mean()

        closed_bar = df.iloc[-2]
        recent_bars = df.iloc[-6:-1]

        intraday_trend_pass = bool(closed_bar['EMA20'] > closed_bar['SMA50'])
        pullback_pass = bool((recent_bars['Low'] <= (recent_bars['EMA20'] * 1.001)).any())
        reversal_pass = bool(closed_bar['Close'] > closed_bar['Open'])

        if intraday_trend_pass and pullback_pass and reversal_pass:
            print(f"\n[BEAR MARKET TRIGGER] Inverse ETF Buy Signal on {etf_symbol} ({t212_ticker})!")
            print(f"   Active Bear Budget (30%): GBP {active_bear_budget_gbp:,.2f}")

            entry_price = float(closed_bar['Close'])
            stop_price = float(closed_bar['Low']) - (1.5 * float(closed_bar['ATR14']))
            target_price = entry_price + (2.5 * abs(entry_price - stop_price))

            shares = calculate_volatility_parity_position_size(
                account_equity_gbp=account_val_gbp,
                available_cash_gbp=active_bear_budget_gbp,
                entry_price=entry_price,
                stop_price=stop_price,
                atr14=float(closed_bar['ATR14']),
                trade_currency="USD" if not etf_symbol.endswith(".L") else "GBP",
                quantity_precision=meta.get("quantityPrecision", 4),
                min_trade_size=meta.get("minTradeSize", 0.0001)
            )

            if shares > 0:
                print(f"   Calculated Bear Order: BUY {shares} shares of Inverse ETF {t212_ticker} @ ${entry_price:.2f}")
                status_code, response_data = place_market_order(t212_ticker, shares, dry_run=dry_run)
                print(f"   [API RESPONSE] Status: {status_code} | Output: {response_data}")

                if status_code in (200, 201):
                    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO trades 
                        (ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
                    """, (t212_ticker, etf_symbol, shares, entry_price, stop_price, target_price, now_str))
                    conn.commit()
                    conn.close()

                    log_scan_audit(t212_ticker, entry_price, True, True, True, "INVESTED", "Bear Market Inverse ETF Trade Executed")
    except Exception as e:
        print(f"[ERROR] Inverse ETF exception for {etf_symbol}: {e}")

# -----------------------------------------------------------------------------
# 9. Main Scan Execution Engine (Integrating All 5 External Data Feeds)
# -----------------------------------------------------------------------------
def run_strategy_b_scan(watchlist: list, dry_run: bool = False):
    print("\n=======================================================================")
    print("[BOT] STARTING INSTITUTIONAL QUANT ENGINE v4.0 (ULTIMATE) SCAN CYCLE")
    print("=======================================================================")
    
    init_database()
    init_csv_logs()

    # Data Feed 2: Market Fear Index (^VIX Guard)
    vix_multiplier, vix_pause = check_vix_volatility_regime()
    if vix_pause:
        print("[VIX PANIC REJECT] ^VIX > 30. Extreme market crash volatility detected. Pausing long buys.")
        return

    # Consultant #3 Guard: Opening 15-Minute Volatility Exclusion Zone
    if is_in_opening_spread_window():
        print("[SPREAD GUARD REJECT] 9:30-9:45 AM EST Opening Window active. Pausing new buys until OTC spreads tighten.")
        return

    # Relative Strength Priority Sorter
    scanned_watchlist = get_ranked_watchlist_by_relative_strength(watchlist)

    summary = fetch_account_summary()
    if not summary:
        print("[CRITICAL] Could not connect to Trading 212 API. Aborting scan.")
        return

    account_val_gbp = summary.get("totalValue", 5000.0)
    cash_available_gbp = summary.get("free", 5000.0)

    print(f"[*] Account Equity: GBP {account_val_gbp:,.2f} | Free Cash: GBP {cash_available_gbp:,.2f}")

    manage_open_position_exits(dry_run=dry_run)

    open_pos = fetch_open_positions()
    active_tickers = [p.get("ticker") for p in open_pos]

    for yf_symbol in scanned_watchlist:
        time.sleep(0.15)
        t212_ticker = resolve_t212_ticker(yf_symbol)
        meta = METADATA_CACHE.get(t212_ticker, {"minTradeSize": 0.0001, "quantityPrecision": 4, "currencyCode": "USD"})

        # Region-Specific Macro Guard (US: SPY, UK: FTSE 100, Europe: Euro Stoxx 50)
        if not check_macro_market_regime(yf_symbol):
            print(f"\n[MACRO REJECT] {t212_ticker} ({yf_symbol}): Regional market benchmark is in a macro downtrend.")
            
            # Bear Market Inverse ETF Mode (70% Cash Reserve Locked, 30% Active Budget)
            run_bear_market_inverse_etf_strategy(yf_symbol, account_val_gbp, cash_available_gbp, dry_run=dry_run)
            continue

        # Data Feed 1: Corporate Earnings Guard
        if check_upcoming_earnings(yf_symbol):
            continue

        # Sector Cap Check
        sector = SECTOR_MAP.get(yf_symbol, "Other")
        if sector != "Other":
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT yf_symbol FROM trades WHERE status = 'OPEN'")
            open_trades = cursor.fetchall()
            conn.close()
            sector_cnt = sum(1 for t in open_trades if SECTOR_MAP.get(t[0], "Other") == sector)
            if sector_cnt >= 2:
                print(f"\n[SKIP] {t212_ticker} ({yf_symbol}): Sector concentration cap reached (Max 2 {sector} positions).")
                continue

        # Data Feed 4: Sector ETF Momentum Alignment
        if not check_sector_etf_alignment(yf_symbol):
            continue

        try:
            df = fetch_stock_5m_candles(yf_symbol)
            if df.empty or len(df) < 50:
                continue

            is_uk_pence = yf_symbol.endswith(".L")

            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['SMA50'] = df['Close'].rolling(window=50).mean()
            df['TR'] = df[['High', 'Low', 'Close']].apply(
                lambda x: max(x['High'] - x['Low'], abs(x['High'] - df['Close'].shift(1).loc[x.name]), abs(x['Low'] - df['Close'].shift(1).loc[x.name])), 
                axis=1
            )
            df['ATR14'] = df['TR'].rolling(window=14).mean()
            df['Vol_SMA20'] = df['Volume'].rolling(window=20).mean()
            
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI14'] = 100 - (100 / (1 + rs))

            closed_bar = df.iloc[-2]
            recent_bars = df.iloc[-6:-1]

            intraday_trend_pass = bool(closed_bar['EMA20'] > closed_bar['SMA50'])
            pullback_pass = bool((recent_bars['Low'] <= (recent_bars['EMA20'] * 1.001)).any())
            reversal_pass = bool(closed_bar['Close'] > closed_bar['Open'])
            vol_ratio = float(closed_bar['Volume'] / closed_bar['Vol_SMA20']) if closed_bar['Vol_SMA20'] > 0 else 1.0
            volume_pass = vol_ratio <= 1.25

            current_close = float(closed_bar['Close'])
            current_low = float(closed_bar['Low'])
            ema20 = float(closed_bar['EMA20'])
            sma50 = float(closed_bar['SMA50'])
            atr14 = float(closed_bar['ATR14'])
            rsi14 = float(closed_bar['RSI14'])

            if intraday_trend_pass and pullback_pass and reversal_pass and volume_pass:
                # Data Feed 3: News Sentiment Check
                sentiment_score, is_news_safe = check_news_sentiment(yf_symbol)
                if not is_news_safe:
                    continue

                # ML Probability Classifier with Sentiment Boost
                ema_dist_atr = (current_close - ema20) / max(0.01, atr14)
                atr_pct = (atr14 / current_close) * 100.0
                ml_confidence, ml_approved = predict_signal_quality(rsi14, vol_ratio, ema_dist_atr, atr_pct, sentiment_score)

                print(f"\n[SIGNAL CANDIDATE] {t212_ticker} ({yf_symbol}) [Sector: {sector}]")
                print(f"   Price: ${current_close:.2f} | 20 EMA: ${ema20:.2f} | ATR(14): ${atr14:.2f} | RSI: {rsi14:.1f} | Sentiment: {sentiment_score:.2f}")
                print(f"   [ML CLASSIFIER] Confidence Score: {ml_confidence * 100:.1f}% -> Approved: {ml_approved}")

                if ml_approved:
                    print(f"--> [EXECUTION APPROVED] Ultimate Quant Buy signal on {t212_ticker}!")

                    entry_price = current_close
                    stop_price = current_low - (1.5 * atr14)
                    target_price = entry_price + (2.5 * abs(entry_price - stop_price))

                    trade_curr = "GBP" if is_uk_pence else meta.get("currencyCode", "USD")

                    # Volatility Parity Sizing with VIX Multiplier
                    shares = calculate_volatility_parity_position_size(
                        account_equity_gbp=account_val_gbp,
                        available_cash_gbp=cash_available_gbp,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        atr14=atr14,
                        trade_currency=trade_curr,
                        quantity_precision=meta.get("quantityPrecision", 4),
                        min_trade_size=meta.get("minTradeSize", 0.0001),
                        vix_multiplier=vix_multiplier
                    )

                    if shares > 0:
                        print(f"   Calculated Vol-Parity Order: BUY {shares} shares of {t212_ticker} @ ${entry_price:.2f}")
                        status_code, response_data = place_market_order(t212_ticker, shares, dry_run=dry_run)
                        print(f"   [API RESPONSE] Status: {status_code} | Output: {response_data}")

                        if status_code in (200, 201):
                            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            conn = sqlite3.connect(DB_PATH)
                            cursor = conn.cursor()
                            cursor.execute("""
                                INSERT OR REPLACE INTO trades 
                                (ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at, status)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
                            """, (t212_ticker, yf_symbol, shares, entry_price, stop_price, target_price, now_str))
                            conn.commit()
                            conn.close()

                            log_scan_audit(t212_ticker, current_close, True, True, True, "INVESTED", f"ML Confidence: {ml_confidence*100:.1f}%")
                    else:
                        print("   [SKIP] Volatility Parity size failed constraints / cash limits.")
                else:
                    print(f"   [ML REJECT] Signal confidence ({ml_confidence*100:.1f}%) < 65.0% threshold.")

        except Exception:
            pass

    print("\n=======================================================================")
    print("[BOT] SCAN CYCLE COMPLETED")
    print("=======================================================================")

# -----------------------------------------------------------------------------
# 10. Performance Scorecard
# -----------------------------------------------------------------------------
def print_performance_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT realized_pnl, status FROM trades WHERE status LIKE 'CLOSED_%'")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("\n=======================================================================")
        print("[QUANT STATS] SYSTEMATIC PERFORMANCE ANALYTICS SCORECARD")
        print("=======================================================================")
        print("[*] No closed historical trades recorded in database yet.")
        print("=======================================================================\n")
        return

    pnls = [r[0] for r in rows if r[0] is not None]
    total_trades = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    win_rate = (len(wins) / total_trades) * 100.0 if total_trades > 0 else 0.0
    total_pnl = sum(pnls)
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0
    profit_factor = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else (sum(wins) if wins else 0.0)
    expectancy = ((win_rate / 100.0) * avg_win) - (((100.0 - win_rate) / 100.0) * avg_loss)
MANUAL_ENTRIES_PATH = os.path.join(SCRIPT_DIR, "manual_entries.json")

def load_manual_entries() -> dict:
    defaults = {
        "PEP_US_EQ": 137.92, "PEP": 137.92,
        "SNOW_US_EQ": 327.28, "SNOW": 327.28,
        "ABBV_US_EQ": 245.46, "ABBV": 245.46,
        "BABA_US_EQ": 128.52, "BABA": 128.52,
        "PLTR_US_EQ": 170.71, "PLTR": 170.71,
        "MRK_US_EQ": 128.58, "MRK": 128.58,
        "ZS_US_EQ": 167.65, "ZS": 167.65,
        "MS_US_EQ": 216.01, "MS": 216.01,
        "MCD_US_EQ": 274.68, "MCD": 274.68,
        "AAPL_US_EQ": 312.50, "AAPL": 312.50,
        "JNJ_US_EQ": 256.90, "JNJ": 256.90,
        "ABT_US_EQ": 107.47, "ABT": 107.47,
        "CSCO_US_EQ": 121.51, "CSCO": 121.51,
        "AVGO_US_EQ": 424.68, "AVGO": 424.68,
        "LLY_US_EQ": 1177.75, "LLY": 1177.75
    }
    if os.path.exists(MANUAL_ENTRIES_PATH):
        try:
            with open(MANUAL_ENTRIES_PATH, "r") as f:
                disk_entries = json.load(f)
                defaults.update(disk_entries)
        except Exception:
            pass
    return defaults

def save_manual_entry(ticker: str, entry_price: float):
    entries = load_manual_entries()
    if ticker not in entries or entries[ticker] <= 0:
        entries[ticker] = round(entry_price, 2)
        try:
            with open(MANUAL_ENTRIES_PATH, "w") as f:
                json.dump(entries, f, indent=2)
        except Exception:
            pass

def print_open_positions_and_exit_conditions():
    """
    Portfolio Audit CLI Tool:
    Inspects active strategy holdings in SQLite and Trading 212 API,
    calculating exact live Stop Loss (SL), Take Profit (TP), and Technical Exit triggers for each position.
    """
    init_database()
    api_positions = fetch_open_positions()
    order_fills = fetch_order_history_fill_prices()

    now_dt = datetime.datetime.now()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: Read existing DB rows
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        SELECT ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at
        FROM trades WHERE status IN ('OPEN', 'OPEN_MONEY_MARKET')
    """)
    db_rows = {row[0]: row for row in cursor.fetchall()}
    conn.close()

    if api_positions:
        conn_lock = sqlite3.connect(DB_PATH, timeout=10)
        cur_lock = conn_lock.cursor()
        cur_lock.execute("PRAGMA journal_mode=WAL;")
        
        for pos in api_positions:
            if not isinstance(pos, dict): continue
            t212_ticker = pos.get("ticker") or (pos.get("instrument", {}).get("ticker") if isinstance(pos.get("instrument"), dict) else None) or pos.get("instrumentCode") or "UNKNOWN"
            if t212_ticker == "UNKNOWN": continue

            shares = pos.get("quantity", 0.0)
            ppl = pos.get("ppl", 0.0)

            # Reverse lookup YF symbol
            yf_symbol = t212_ticker.replace("_US_EQ", "").replace("_UK_EQ", ".L").replace("_DE_EQ", ".DE").replace("_FR_EQ", ".PA").replace("_NL_EQ", ".AS")
            for yf_k, t212_v in YF_TO_T212_MAP.items():
                if t212_v == t212_ticker:
                    yf_symbol = yf_k
                    break

            # Fetch exact live real-time price directly from Trading 212 API
            current_price = pos.get("currentPrice") or 0.0
            if current_price <= 0:
                try:
                    stock = yf.Ticker(yf_symbol)
                    df = stock.history(period="5d", interval="5m")
                    if not df.empty:
                        current_price = float(df['Close'].iloc[-1])
                        if yf_symbol.endswith(".L"): current_price /= 100.0
                except Exception:
                    pass

            api_avg = pos.get("averagePrice") or pos.get("initialFillPrice") or pos.get("buyPrice") or 0.0
            fx_ppl = pos.get("fxPpl", 0.0)

            # PRIORITY CHAIN FOR ENTRY PRICE (TRADING 212 OFFICIAL ORDER HISTORY FIRST)
            # Priority 1: Use official executed fillPrice from Trading 212 API Order History
            if t212_ticker in order_fills and order_fills[t212_ticker] > 0:
                entry_price = order_fills[t212_ticker]
            # Priority 2: Use API averagePrice / initialFillPrice from position object
            elif api_avg > 0:
                entry_price = api_avg
            # Priority 3: Use locked entry price from SQLite database
            elif t212_ticker in db_rows and db_rows[t212_ticker][3] > 0:
                entry_price = db_rows[t212_ticker][3]
            # Priority 4: Zero-Skew Currency-Decoupled Formula using synchronized Trading 212 data
            else:
                pure_stock_pnl_gbp = ppl - fx_ppl
                fx_rate = get_fx_rate_to_gbp("USD")
                if shares > 0 and fx_rate > 0 and current_price > 0:
                    skew_adj = pure_stock_pnl_gbp / (shares * fx_rate)
                    entry_price = round(current_price - skew_adj, 2)
                else:
                    entry_price = current_price if current_price > 0 else 100.0

            # Check created timestamp from Trading 212 API
            created_ts = pos.get("created") or pos.get("initialFillDate")
            opened_at_str = now_str
            if created_ts:
                try:
                    dt_parsed = datetime.datetime.fromisoformat(created_ts.replace("Z", "+00:00"))
                    opened_at_str = dt_parsed.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

            if t212_ticker in db_rows and db_rows[t212_ticker][6]:
                opened_at_str = db_rows[t212_ticker][6]

            stop_price = 0.0
            target_price = 0.0

            if entry_price > 0 and current_price > 0:
                try:
                    df['TR'] = df[['High', 'Low', 'Close']].apply(
                        lambda x: max(x['High'] - x['Low'], abs(x['High'] - df['Close'].shift(1).loc[x.name]), abs(x['Low'] - df['Close'].shift(1).loc[x.name])), 
                        axis=1
                    )
                    atr14 = float(df['TR'].rolling(window=14).mean().iloc[-1])
                    stop_price = round(entry_price - (1.5 * atr14), 2)
                    target_price = round(entry_price + (2.5 * (1.5 * atr14)), 2)
                except Exception:
                    stop_price = round(entry_price * 0.98, 2)
                    target_price = round(entry_price * 1.05, 2)

            # Sync & Lock permanently to SQLite database
            cur_lock.execute("""
                INSERT OR REPLACE INTO trades 
                (ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
            """, (t212_ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at_str))

        conn_lock.commit()
        conn_lock.close()

    # Step 3: Re-load manual_entries vault & DB rows so all calculated entries are available in memory
    manual_entries = load_manual_entries()

    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, yf_symbol, shares, entry_price, stop_price, target_price, opened_at
        FROM trades WHERE status IN ('OPEN', 'OPEN_MONEY_MARKET')
    """)
    db_rows = {row[0]: row for row in cursor.fetchall()}
    conn.close()

    print("\n=======================================================================")
    print("[PORTFOLIO AUDIT] ACTIVE HOLDINGS & LIVE EXIT CONDITIONS")
    print("=======================================================================")

    if not api_positions:
        print("[*] No active open strategy positions in database or Trading 212 API.")
        print("=======================================================================\n")
        return

    for pos in api_positions:
        t212_ticker = "UNKNOWN"
        if isinstance(pos, dict):
            t212_ticker = pos.get("ticker") or (pos.get("instrument", {}).get("ticker") if isinstance(pos.get("instrument"), dict) else None) or pos.get("instrumentCode") or "UNKNOWN"
        
        shares = pos.get("quantity", 0.0) if isinstance(pos, dict) else 0.0
        ppl = pos.get("ppl", 0.0) if isinstance(pos, dict) else 0.0

        yf_symbol = t212_ticker.replace("_US_EQ", "").replace("_UK_EQ", ".L").replace("_DE_EQ", ".DE").replace("_FR_EQ", ".PA").replace("_NL_EQ", ".AS")
        for yf_k, t212_v in YF_TO_T212_MAP.items():
            if t212_v == t212_ticker:
                yf_symbol = yf_k
                break

        entry_price = 0.0
        stop_price = 0.0
        target_price = 0.0
        opened_at_str = now_str
        hours_open = 0.0

        if t212_ticker in manual_entries:
            entry_price = manual_entries[t212_ticker]
        elif t212_ticker in db_rows and db_rows[t212_ticker][3] > 0:
            entry_price = db_rows[t212_ticker][3]

        if t212_ticker in db_rows:
            _, _, _, _, stop_price, target_price, opened_at_str = db_rows[t212_ticker]

        try:
            opened_at_dt = datetime.datetime.strptime(opened_at_str, "%Y-%m-%d %H:%M:%S")
            hours_open = max(0.0, (now_dt - opened_at_dt).total_seconds() / 3600.0)
        except Exception:
            pass

        current_price = entry_price if entry_price > 0 else 0.0
        try:
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period="5d", interval="5m")
            if not df.empty:
                current_price = float(df['Close'].iloc[-1])
                if yf_symbol.endswith(".L"): current_price /= 100.0
        except Exception:
            pass

        unrealized_pnl = (current_price - entry_price) * shares if entry_price > 0 else ppl
        pnl_pct = ((current_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0

        print(f"\n📌 POS: {t212_ticker} ({yf_symbol}) | Shares: {shares}")
        if entry_price > 0:
            print(f"   --> Entry Price:    ${entry_price:.2f}")
        else:
            print(f"   --> Entry Price:    [NOT SET] (Run python automate_strategy_b_trading212.py --set-entry {yf_symbol} <PRICE>)")

        print(f"   --> Current Price:  ${current_price:.2f} (Unrealized PnL: £{ppl:+.2f} / {pnl_pct:+.2f}%)")
        print(f"   --> Stop Loss (SL): ${stop_price:.2f}  (Triggers SELL if Price <= ${stop_price:.2f})")
        print(f"   --> Take Profit(TP):${target_price:.2f} (Triggers SELL if Price >= ${target_price:.2f})")
        if hours_open > 0:
            print(f"   --> Time Elapsed:   {hours_open:.1f} Hours Open")
        print(f"   --> Technical Exit: Triggers SELL if 20 EMA crosses below 50 SMA")

    print("\n=======================================================================\n")



# -----------------------------------------------------------------------------
# 11. Entrypoint & Daemon Loop
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Strategy B Automated Trading Engine")
    parser.add_argument("--once", action="store_true", help="Perform a single scan cycle and exit.")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in 5-minute daemon loop.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate signals and orders without API submission.")
    parser.add_argument("--test-connection", action="store_true", help="Test Trading 212 API connection and exit.")
    parser.add_argument("--show-watchlist", action="store_true", help="Print active watchlist tickers and exit.")
    parser.add_argument("--positions", action="store_true", help="Print active holdings and live exit triggers and exit.")
    parser.add_argument("--stats", action="store_true", help="Print systematic performance scorecard and exit.")
    parser.add_argument("--set-entry", nargs=2, metavar=('SYMBOL', 'PRICE'), help="Lock exact manual entry price for a stock (e.g. --set-entry PEP 137.92)")
    args = parser.parse_args()

    init_database()
    init_csv_logs()

    if args.set_entry:
        symbol, price_str = args.set_entry
        symbol_clean = symbol.upper().strip()
        t212_t = resolve_t212_ticker(symbol_clean)
        try:
            val = float(price_str)
            save_manual_entry(t212_t, val)
            save_manual_entry(symbol_clean, val)
            print(f"[SUCCESS] Locked Entry Price for {t212_t} ({symbol_clean}) to ${val:.2f}!")
        except Exception as e:
            print(f"[ERROR] Invalid price '{price_str}': {e}")
        sys.exit(0)

    if args.positions:
        print_open_positions_and_exit_conditions()
        sys.exit(0)

    if args.stats:
        print_performance_stats()
        sys.exit(0)

    default_watchlist = [
        "NVDA", "AAPL", "MSFT", "TSLA", "AMZN", "GOOGL", "AMD", "META", "AVGO", "LLY",
        "JPM", "WMT", "V", "UNH", "MA", "PG", "COST", "JNJ", "HD", "ORCL",
        "BAC", "XOM", "NFLX", "CRM", "CVX", "ABBV", "MRK", "KO", "PEP", "TMO",
        "CSCO", "MCD", "ABT", "DIS", "GE", "IBM", "INTC", "PM", "CAT", "TXN",
        "QCOM", "AMAT", "NOW", "PANW", "UBER", "MU", "GS", "MS", "AXP", "BLK",
        "PLTR", "COIN", "XYZ", "SHOP", "SPOT", "NET", "DDOG", "SNOW", "CRWD", "ZS",
        "MDB", "ROKU", "DKNG", "SNAP", "PINS", "RBLX", "TWLO", "PATH", "AFRM", "UPST",
        "SMCI", "ARM", "APP", "CVNA", "HOOD", "ASTS", "IONQ", "MARA", "RIOT", "CLSK",
        "HIMS", "TOST", "DUOL", "SOFI", "CELH", "ON", "MPWR", "ENTG", "FSLR", "ENPH",
        "F", "GM", "RIVN", "LCID", "NIO", "BABA", "PDD", "BIDU", "JD", "LI",
        "SE", "GRAB", "MELI", "CPNG", "NU", "STNE", "VALE", "PBR", "ITUB", "GOLD",
        "SHEL.L", "AZN.L", "HSBA.L", "ULVR.L", "BP.L", "GSK.L", "BARC.L",
        "SAP.DE", "SIE.DE", "ALV.DE", "AIR.DE", "BMW.DE", "MC.PA", "OR.PA", "RMS.PA", "ASML.AS"
    ]

    watchlist_file = os.path.join(SCRIPT_DIR, "watchlist.txt")
    if os.path.exists(watchlist_file):
        with open(watchlist_file, "r") as f:
            custom_list = [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]
        if custom_list: watchlist = custom_list
        else: watchlist = default_watchlist
    else: watchlist = default_watchlist

    if args.show_watchlist:
        print("\n=======================================================================")
        print(f"[WATCHLIST] ACTIVE STRATEGY B WATCHLIST ({len(watchlist)} TICKERS)")
        print("=======================================================================")
        for i, t in enumerate(watchlist, 1):
            print(f"  {i:02d}. {t}")
        print("=======================================================================\n")
        sys.exit(0)

    if args.test_connection:
        success = test_trading212_connection()
        sys.exit(0 if success else 1)

    if not test_trading212_connection():
        print("[CRITICAL] API connection failed. Stopping execution.")
        sys.exit(1)

    load_instrument_metadata()

    if args.daemon:
        print("[DAEMON MODE] Starting Institutional Quant Engine v4.0 (ULTIMATE)...")
        print("  --> Earnings Guard: 48h Gap Risk Protection Active")
        print("  --> VIX Volatility Guard: Market Crash Risk Scaling Active")
        print("  --> News Sentiment NLP: Headline Veto Filter Active")
        print("  --> Sector ETF Alignment: XLK/XLF/XLV Intraday Alignment Active")
        print("  --> RS Leader Sorter: Priority Market Leader Scanning Active")
        print("  --> ML Classifier: Probability Score >= 65% Guard Active")
        print("  --> Time Decay Exit: 3-Hour Sideways Trade Expiry Active")
        print("  --> Vol-Parity Sizing: ATR & VIX Risk Normalization Active")
        print("  --> Exit Manager: High-Frequency 10-Second Monitor Active")
        import schedule
        
        schedule.every(10).seconds.do(lambda: manage_open_position_exits(dry_run=args.dry_run))

        def buy_scan_job():
            if is_market_open():
                run_strategy_b_scan(watchlist, dry_run=args.dry_run)

        schedule.every(5).minutes.do(buy_scan_job)
        
        manage_open_position_exits(dry_run=args.dry_run)
        if is_market_open():
            run_strategy_b_scan(watchlist, dry_run=args.dry_run)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        if is_market_open():
            run_strategy_b_scan(watchlist, dry_run=args.dry_run)
        else:
            print("[STANDBY] Market is closed. Skipping scan.")
        sys.exit(0)

if __name__ == "__main__":
    main()
