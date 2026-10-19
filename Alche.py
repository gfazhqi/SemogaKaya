def update_bot_spread_logic():
    code = """import yfinance as yf
import ccxt
import time
import requests
import datetime

# =================================================================
# PENGATURAN BOT
# =================================================================

# 1. Konfigurasi Bot Telegram
TELEGRAM_BOT_TOKEN = "MASUKKAN_TOKEN_BOT_TELEGRAM_KAMU_DISINI"
TELEGRAM_CHAT_ID = "MASUKKAN_CHAT_ID_KAMU_DISINI"

# 2. Daftar Pair yang Dipantau
CRYPTO_PAIRS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
FOREX_PAIRS = ['GC=F', 'EURUSD=X', 'GBPUSD=X', 'CL=F']

ALL_PAIRS = CRYPTO_PAIRS + FOREX_PAIRS

# 3. Pengaturan Strategi Alchemist
TF_HTF_CRYPTO = '1h'    
TF_EXEC_CRYPTO = '15m'  
EMA_PERIOD = 50

TF_HTF_FOREX = '60m'
TF_EXEC_FOREX = '15m'

# Kumpulan Ukuran Tick (Desimal)
TICK_SIZES = {
    'BTC/USDT': 0.1, 'ETH/USDT': 0.01, 'SOL/USDT': 0.001, 'BNB/USDT': 0.01, 'XRP/USDT': 0.0001,
    'GC=F': 0.1,        # Gold (1 tick = 0.1)
    'EURUSD=X': 0.00001, # Forex 5 digit (1 pipette/tick = 0.00001)
    'GBPUSD=X': 0.00001,
    'CL=F': 0.01
}

# =================================================================
# LOGIKA STOP LOSS (BUFFER + SPREAD EXNESS)
# =================================================================
# Sesuai logika asli Pine Script: SL ditaruh di ujung ekor candle + Buffer
SL_BUFFER_TICKS = 15  # Buffer bawaan SMC (15 ticks = 1.5 pips)

# Tambahan Spread Khusus Exness (Zero/Raw ~0.2 pips = 2 ticks)
SPREAD_TICKS_FOREX = 2  
SPREAD_TICKS_GOLD = 2   

# Kill Zone (Waktu Server/UTC)
LON_START = 7
LON_END = 10
NY_START = 12
NY_END = 15

exchange = ccxt.binance()

def send_telegram_msg(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        requests.post(url, data=payload)
    except:
        pass

def check_kill_zone():
    current_hour = datetime.datetime.utcnow().hour
    return (LON_START <= current_hour < LON_END) or (NY_START <= current_hour < NY_END)

def calculate_ema(prices, period=50):
    if len(prices) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def scan_pair_crypto(symbol):
    try:
        ohlcv_1h = exchange.fetch_ohlcv(symbol, TF_HTF_CRYPTO, limit=60)
        ohlcv_15m = exchange.fetch_ohlcv(symbol, TF_EXEC_CRYPTO, limit=3)
        if not ohlcv_1h or not ohlcv_15m: return

        htf_high, htf_low = ohlcv_1h[-2][2], ohlcv_1h[-2][3]
        ema_50 = calculate_ema([x[4] for x in ohlcv_1h], EMA_PERIOD)
        
        cur_open, cur_high, cur_low, cur_close = ohlcv_15m[-2][1:5]
        
        is_bullish = cur_close > ema_50
        is_bearish = cur_close < ema_50
        
        tick = TICK_SIZES.get(symbol, 0.001)
        sl_buffer = tick * SL_BUFFER_TICKS # Crypto hanya pakai buffer bawaan

        if cur_high > htf_high and cur_close < htf_high and cur_close < cur_open and is_bearish:
            entry = (cur_high + cur_low) / 2
            sl = cur_high + sl_buffer
            tp = entry - (sl - entry)
            msg = f"🔴 *SELL {symbol}*\nTF: {TF_EXEC_CRYPTO}\nEntry (50%): {entry:.5f}\nSL: {sl:.5f}\nTP (1R): {tp:.5f}"
            send_telegram_msg(msg)

        elif cur_low < htf_low and cur_close > htf_low and cur_close > cur_open and is_bullish:
            entry = (cur_high + cur_low) / 2
            sl = cur_low - sl_buffer
            tp = entry + (entry - sl)
            msg = f"🟢 *BUY {symbol}*\nTF: {TF_EXEC_CRYPTO}\nEntry (50%): {entry:.5f}\nSL: {sl:.5f}\nTP (1R): {tp:.5f}"
            send_telegram_msg(msg)

    except Exception as e:
        print(f"Error Crypto {symbol}: {e}")

def scan_pair_forex(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df_1h = ticker.history(interval=TF_HTF_FOREX, period="5d")
        df_15m = ticker.history(interval=TF_EXEC_FOREX, period="2d")
        if len(df_1h) < 50 or len(df_15m) < 3: return
            
        htf_candle = df_1h.iloc[-2]
        htf_high, htf_low = htf_candle['High'], htf_candle['Low']
        ema_50 = calculate_ema(df_1h['Close'].tolist(), EMA_PERIOD)
        
        cur_candle = df_15m.iloc[-2]
        cur_open, cur_high, cur_low, cur_close = cur_candle['Open'], cur_candle['High'], cur_candle['Low'], cur_candle['Close']

        is_bullish = cur_close > ema_50
        is_bearish = cur_close < ema_50
        
        tick = TICK_SIZES.get(symbol, 0.00001)
        
        # LOGIKA SL: Buffer SMC (15) + Spread Exness (2) = 17 Ticks
        if symbol == 'GC=F': 
             sl_buffer = tick * (SL_BUFFER_TICKS + SPREAD_TICKS_GOLD)
        else: 
             sl_buffer = tick * (SL_BUFFER_TICKS + SPREAD_TICKS_FOREX)

        if cur_high > htf_high and cur_close < htf_high and cur_close < cur_open and is_bearish:
            entry = (cur_high + cur_low) / 2
            sl = cur_high + sl_buffer
            tp = entry - (sl - entry)
            clean_name = "XAUUSD" if symbol == 'GC=F' else symbol.replace('=X', '')
            msg = f"🔴 *SELL {clean_name} (Forex)*\nTF: {TF_EXEC_FOREX}\nEntry (50%): {entry:.5f}\nSL: {sl:.5f}\nTP (1R): {tp:.5f}"
            send_telegram_msg(msg)

        elif cur_low < htf_low and cur_close > htf_low and cur_close > cur_open and is_bullish:
            entry = (cur_high + cur_low) / 2
            sl = cur_low - sl_buffer
            tp = entry + (entry - sl)
            clean_name = "XAUUSD" if symbol == 'GC=F' else symbol.replace('=X', '')
            msg = f"🟢 *BUY {clean_name} (Forex)*\nTF: {TF_EXEC_FOREX}\nEntry (50%): {entry:.5f}\nSL: {sl:.5f}\nTP (1R): {tp:.5f}"
            send_telegram_msg(msg)

    except Exception as e:
        print(f"Error Forex {symbol}: {e}")

print("🚀 Bot Alchemist Python (Crypto + Forex) Berjalan...")

while True:
    try:
        if check_kill_zone():
            for pair in CRYPTO_PAIRS:
                scan_pair_crypto(pair)
                time.sleep(1)
            for pair in FOREX_PAIRS:
                scan_pair_forex(pair)
                time.sleep(1)
        time.sleep(900) 
    except KeyboardInterrupt:
        break
    except:
        time.sleep(60)
"""
    return code

code = update_bot_spread_logic()
with open("/mnt/data/alchemist_bot_exness_v2.py", "w") as f:
    f.write(code)
print("[file-tag: code-generated-file-alchemist-bot-exness-v2]")

