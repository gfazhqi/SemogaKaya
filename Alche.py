import os
import time
import sqlite3
import threading
from datetime import datetime
import pandas as pd
import requests
from flask import Flask, request
import ccxt

# =================================================================
# MODUL MT5 UNTUK FOREX
# =================================================================
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False

app = Flask(__name__)

# =================================================================
# KONFIGURASI BOT & AKUN (ALCHEMIST PRO HYBRID V2)
# =================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "MASUKKAN_TOKEN_BOT_TELEGRAM")
TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", "MASUKKAN_CHAT_ID_TELEGRAM"))

API_KEY = os.getenv("API_KEY", "MASUKKAN_API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY", "MASUKKAN_SECRET_KEY")

# Mode Entry: "retrace" (Retrace 50% - SL Tipis) atau "market" (Market Close - Agresif)
ENTRY_MODE = "retrace"

# Parameter Crypto (BingX)
CRYPTO_MARGIN_USDT = 2.0
CRYPTO_LEVERAGE = 50
TOP_CRYPTO_COUNT = 50

# Parameter Forex (MT5)
FOREX_PAIRS = ["XAUUSD", "GBPUSD", "EURUSD"]
FOREX_LOT = 0.01
MAX_SPREAD_PIPS = 0.2
FOREX_LEVERAGE_INFO = 2000

# Parameter Logika SMC Alchemist
TIMEFRAME_HTF = "1h"
TIMEFRAME_LTF = "15m"
SL_BUFFER_TICKS = 15

# Sesi Market (Kill Zone) - Jam WIB (UTC+7)
LON_START, LON_END = 14, 17
NY_START, NY_END = 19, 22

last_trade_times = {}

# =================================================================
# DATABASE & TELEGRAM
# =================================================================
DB_FILE = "trades.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, action TEXT, entry_price REAL,
            sl_price REAL, tp_price REAL, margin REAL, leverage INTEGER,
            status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def log_trade(symbol, action, entry_price, sl_price, tp_price, margin, leverage, status):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM trades WHERE created_at < datetime('now', '-7 days')")
    c.execute('''
        INSERT INTO trades (symbol, action, entry_price, sl_price, tp_price, margin, leverage, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (symbol, action, entry_price, sl_price, tp_price, margin, leverage, status))
    conn.commit()
    conn.close()

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Gagal kirim notifikasi Telegram: {e}")

# =================================================================
# HELPER & DATA FETCHING
# =================================================================
def get_exchange():
    return ccxt.bingx({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })

def calculate_ema(series, period=50):
    return series.ewm(span=period, adjust=False).mean()

def get_top_crypto_pairs(exchange, limit=50):
    try:
        tickers = exchange.fetch_tickers()
        usdt_pairs = [v for k, v in tickers.items() if '/USDT:USDT' in k]
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x.get('quoteVolume', 0), reverse=True)
        return [pair['symbol'] for pair in sorted_pairs[:limit]]
    except Exception:
        return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

def fetch_ohlcv_crypto(exchange, symbol, timeframe, limit=100):
    bars = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def fetch_ohlcv_forex(symbol, timeframe_str, limit=100):
    if not MT5_AVAILABLE or not mt5.terminal_info(): return None
    tf_map = {"15m": mt5.TIMEFRAME_M15, "1h": mt5.TIMEFRAME_H1, "1d": mt5.TIMEFRAME_D1}
    rates = mt5.copy_rates_from_pos(symbol, tf_map.get(timeframe_str, mt5.TIMEFRAME_M15), 0, limit)
    if rates is None or len(rates) == 0: return None
    df = pd.DataFrame(rates)
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    return df

# =================================================================
# EKSEKUSI ORDER SAMA PRESISI DENGAN SL & TP
# =================================================================
def execute_crypto_order(exchange, symbol, action, entry_price, sl_price, tp_price):
    try:
        exchange.set_leverage(CRYPTO_LEVERAGE, symbol)
    except:
        pass
    
    position_size_usdt = CRYPTO_MARGIN_USDT * CRYPTO_LEVERAGE
    amount = position_size_usdt / entry_price
    
    order_type = 'limit' if ENTRY_MODE == "retrace" else 'market'
    params = {
        'stopLoss': {'triggerPrice': float(sl_price)},
        'takeProfit': {'triggerPrice': float(tp_price)}
    }
    
    if order_type == 'limit':
        order = exchange.create_order(symbol, 'limit', action, amount, entry_price, params=params)
    else:
        order = exchange.create_order(symbol, 'market', action, amount, params=params)
        
    log_trade(symbol, action.upper(), entry_price, sl_price, tp_price, CRYPTO_MARGIN_USDT, CRYPTO_LEVERAGE, 'OPEN')
    return order

def execute_forex_order(symbol, action, entry_price, sl_price, tp_price):
    if not MT5_AVAILABLE: return None
    symbol_info = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not symbol_info or not tick: return None

    # Spread Filter Check
    point_size = symbol_info.point
    current_spread_pips = (tick.ask - tick.bid) / (point_size * 10)
    if current_spread_pips > MAX_SPREAD_PIPS:
        print(f"Skipped {symbol}: Spread {current_spread_pips:.2f} pips > {MAX_SPREAD_PIPS} pips")
        return None

    deviation_points = int(MAX_SPREAD_PIPS * 10)
    
    if ENTRY_MODE == "retrace":
        order_type = mt5.ORDER_TYPE_BUY_LIMIT if action == 'buy' else mt5.ORDER_TYPE_SELL_LIMIT
        trade_action = mt5.TRADE_ACTION_PENDING
        price = entry_price
    else:
        order_type = mt5.ORDER_TYPE_BUY if action == 'buy' else mt5.ORDER_TYPE_SELL
        trade_action = mt5.TRADE_ACTION_DEAL
        price = tick.ask if action == 'buy' else tick.bid

    request_data = {
        "action": trade_action,
        "symbol": symbol,
        "volume": FOREX_LOT,
        "type": order_type,
        "price": float(price),
        "sl": float(sl_price),
        "tp": float(tp_price),
        "deviation": deviation_points,
        "magic": 123456,
        "comment": "Alchemist SMC",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    result = mt5.order_send(request_data)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log_trade(symbol, action.upper(), entry_price, sl_price, tp_price, FOREX_LOT, FOREX_LEVERAGE_INFO, 'OPEN')
        return result
    return None

# =================================================================
# CORE LOGIC: ALCHEMIST PRO HYBRID V2 ENGINE
# =================================================================
def process_asset(symbol, is_forex, exchange=None):
    global last_trade_times
    try:
        if is_forex:
            df_htf = fetch_ohlcv_forex(symbol, TIMEFRAME_HTF, limit=100)
            df_ltf = fetch_ohlcv_forex(symbol, TIMEFRAME_LTF, limit=100)
            df_daily = fetch_ohlcv_forex(symbol, "1d", limit=2)
            tick_size = 0.0001 if "JPY" not in symbol and "XAU" not in symbol else 0.01
        else:
            df_htf = fetch_ohlcv_crypto(exchange, symbol, TIMEFRAME_HTF, limit=100)
            df_ltf = fetch_ohlcv_crypto(exchange, symbol, TIMEFRAME_LTF, limit=100)
            df_daily = fetch_ohlcv_crypto(exchange, symbol, "1d", limit=2)
            tick_size = float(exchange.market(symbol)['info'].get('priceTick', 0.001))

        if df_htf is None or df_ltf is None or df_daily is None: return

        # High & Low HTF (H1 Candle Sebelumnya)
        h_htf = df_htf.iloc[-2]['high']
        l_htf = df_htf.iloc[-2]['low']

        # Hitung EMA 50 pada Timeframe HTF (1 Jam)
        df_htf['ema50'] = calculate_ema(df_htf['close'], 50)
        ema50_htf = df_htf.iloc[-2]['ema50']

        # Daily Open
        daily_open = df_daily.iloc[-1]['open']

        # Candle LTF Saat Ini (M15 Baru Tutup)
        current_bar = df_ltf.iloc[-1]
        c_close, c_high, c_low, c_open, c_time = current_bar['close'], current_bar['high'], current_bar['low'], current_bar['open'], current_bar['timestamp']

        # Tren Filter Alchemist (Berdasarkan HTF EMA 50 & Daily Open)
        is_bull = (c_close > ema50_htf) and (c_close > daily_open)
        is_bear = (c_close < ema50_htf) and (c_close < daily_open)

        # Cek Kill Zone
        current_hour = datetime.now().hour
        in_kz = (LON_START <= current_hour < LON_END) or (NY_START <= current_hour < NY_END)

        # Syarat Sinyal SMC Alchemist
        s_cond = in_kz and (c_high > h_htf) and (c_close < h_htf) and (c_close < c_open) and is_bear
        b_cond = in_kz and (c_low < l_htf) and (c_close > l_htf) and (c_close > c_open) and is_bull

        last_time = last_trade_times.get(symbol)

        if (s_cond or b_cond) and (last_time != c_time):
            action = 'sell' if s_cond else 'buy'

            # Perhitungan Entry, SL, dan TP 1R Sesuai Pine Script
            if ENTRY_MODE == "retrace":
                entry_price = (c_high + c_low) / 2.0
            else:
                entry_price = c_close

            if action == 'sell':
                sl_price = c_high + (SL_BUFFER_TICKS * tick_size)
                r_risk = sl_price - entry_price
                tp_price = entry_price - r_risk
            else:
                sl_price = c_low - (SL_BUFFER_TICKS * tick_size)
                r_risk = entry_price - sl_price
                tp_price = entry_price + r_risk

            # Eksekusi Order
            if is_forex:
                result = execute_forex_order(symbol, action, entry_price, sl_price, tp_price)
                if result is None: return
                tipe_market = "FOREX"
                margin_str = f"Lot {FOREX_LOT} (Lev 1:{FOREX_LEVERAGE_INFO})"
            else:
                execute_crypto_order(exchange, symbol, action, entry_price, sl_price, tp_price)
                tipe_market = "CRYPTO"
                margin_str = f"${CRYPTO_MARGIN_USDT} (Lev {CRYPTO_LEVERAGE}x)"

            last_trade_times[symbol] = c_time
            
            msg = (
                f"🚀 *[ALCHEMIST HYBRID V2 - {tipe_market}]*\n\n"
                f"• *Pair:* `{symbol}`\n"
                f"• *Aksi:* `{action.upper()}` ({'Retrace 50%' if ENTRY_MODE == 'retrace' else 'Market Close'})\n"
                f"• *Harga Entry:* `{entry_price:.5f}`\n"
                f"• *Stop Loss:* `{sl_price:.5f}`\n"
                f"• *Take Profit (1R):* `{tp_price:.5f}`\n"
                f"• *Size:* `{margin_str}`"
            )
            send_telegram(msg)

    except Exception:
        pass

def multi_asset_scanner():
    exchange = get_exchange()
    if MT5_AVAILABLE: mt5.initialize()
    print("🚀 Alchemist Pro HYBRID v2 Engine Running!")
    
    while True:
        try:
            crypto_pairs = get_top_crypto_pairs(exchange, TOP_CRYPTO_COUNT)
            
            if MT5_AVAILABLE and mt5.terminal_info():
                for fx in FOREX_PAIRS:
                    process_asset(fx, is_forex=True)
            
            for coin in crypto_pairs:
                process_asset(coin, is_forex=False, exchange=exchange)
                time.sleep(0.1)

            time.sleep(30)
        except Exception as e:
            print(f"Error Scanner Utama: {e}")
            time.sleep(30)

# =================================================================
# TELEGRAM COMMAND LISTENER
# =================================================================
@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    update = request.get_json()
    if not update or 'message' not in update: return 'ok', 200

    msg_data = update['message']
    text = msg_data.get('text', '').strip()
    chat_id = str(msg_data['chat']['id'])

    if chat_id != TELEGRAM_CHAT_ID: return 'ok', 200

    if text == '/posisi':
        try:
            exchange = get_exchange()
            positions = exchange.fetch_positions()
            open_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]
            if not open_pos:
                send_telegram("📭 Tidak ada posisi Crypto aktif saat ini.")
            else:
                msg = "📊 *POSISI RUNNING BINGX*\n\n"
                for p in open_pos:
                    msg += f"• `{p['symbol']}` ({p.get('side', '').upper()})\n  PnL: `{float(p.get('unrealizedPnl', 0)):+.2f} USDT`\n"
                send_telegram(msg)
        except:
            send_telegram("❌ Gagal mengambil posisi aktif.")

    elif text in ['/stat', '/mingguan']:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT status FROM trades")
        rows = c.fetchall()
        conn.close()
        
        total = len(rows)
        msg = f"📈 *REKAP TRADING (7 HARI)*\n\n• *Total Executed Signals:* `{total}`\n"
        send_telegram(msg)

    return 'ok', 200

if __name__ == '__main__':
    init_db()
    scanner_thread = threading.Thread(target=multi_asset_scanner)
    scanner_thread.daemon = True
    scanner_thread.start()
    
    app.run(host='0.0.0.0', port=5000)
