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

ENTRY_MODE = "retrace"  # "retrace" (Retrace 50% - SL Tipis) atau "market" (Market Close)

# Parameter Trading Crypto (BingX)
CRYPTO_MARGIN_USDT = 2.0
CRYPTO_LEVERAGE = 50
TOP_CRYPTO_COUNT = 50

# Parameter Trading Forex (MT5)
FOREX_PAIRS = ["XAUUSD", "GBPUSD", "EURUSD"]
FOREX_LOT = 0.01
MAX_SPREAD_PIPS = 0.2
FOREX_LEVERAGE_INFO = 2000

# Manajemen Risiko Saldo & Floating
MAX_TOTAL_FLOATING_LOSS = -1.5  # Batas maks total floating minus (USDT)

# Parameter Logika SMC Alchemist
TIMEFRAME_HTF = "1h"
TIMEFRAME_LTF = "15m"
SL_BUFFER_TICKS = 15
MAX_PENDING_BARS = 30  # Batas maksimal 30 candle M15 (7.5 jam)

# Sesi Market (Kill Zone) - Jam WIB (UTC+7)
LON_START, LON_END = 14, 17
NY_START, NY_END = 19, 22

# State Bot
IS_BOT_PAUSED = False
last_trade_times = {}

# Tracking Order Pending Aktif (Sama dengan state Pine Script)
# Format: {symbol: {'order_id': id, 'action': 'sell'/'buy', 'entry': p, 'sl': p, 'tp': p, 'created_at_time': t, 'bars_count': 0, 'is_forex': bool}}
pending_orders_tracker = {}

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
        print(f"Gagal kirim Telegram: {e}")

# =================================================================
# HELPER DATA & MANAJEMEN RISIKO OTOMATIS
# =================================================================
def get_exchange():
    return ccxt.bingx({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })

def check_dynamic_position_limit(exchange):
    try:
        balance = exchange.fetch_balance()
        free_usdt = float(balance['USDT']['free'])
        positions = exchange.fetch_positions()
        
        open_positions = [p for p in positions if float(p.get('contracts', 0)) > 0]
        open_pos_count = len(open_positions)
        
        total_unrealized_pnl = sum([float(p.get('unrealizedPnl', 0)) for p in open_positions])
        if total_unrealized_pnl < MAX_TOTAL_FLOATING_LOSS:
            return False, open_pos_count, 0, free_usdt

        max_allowed_positions = max(1, int(free_usdt // 5.0))
        if open_pos_count >= max_allowed_positions:
            return False, open_pos_count, max_allowed_positions, free_usdt
            
        return True, open_pos_count, max_allowed_positions, free_usdt
    except Exception:
        return True, 0, 2, 0.0

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
# EKSEKUSI ORDER (PENDING & MARKET)
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

    point_size = symbol_info.point
    current_spread_pips = (tick.ask - tick.bid) / (point_size * 10)
    if current_spread_pips > MAX_SPREAD_PIPS: return None

    deviation_points = int(MAX_SPREAD_PIPS * 10)
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if (ENTRY_MODE == "retrace" and action == 'buy') else (
        mt5.ORDER_TYPE_SELL_LIMIT if (ENTRY_MODE == "retrace" and action == 'sell') else (
            mt5.ORDER_TYPE_BUY if action == 'buy' else mt5.ORDER_TYPE_SELL
        )
    )
    trade_action = mt5.TRADE_ACTION_PENDING if ENTRY_MODE == "retrace" else mt5.TRADE_ACTION_DEAL
    price = entry_price if ENTRY_MODE == "retrace" else (tick.ask if action == 'buy' else tick.bid)

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
# MESIN PENGELOLA ORDER PENDING (AUTO-CANCEL PRESISI PINE SCRIPT)
# =================================================================
def manage_pending_orders(exchange):
    """
    Memeriksa semua Limit Order pending dan mengeksekusi pembatalan otomatis
    jika terdeteksi: MISSED (TP Hit First), CANCELLED (SL Hit First), atau INVALID (> 30 Bars)
    """
    global pending_orders_tracker
    
    symbols_to_remove = []
    
    for symbol, order_info in list(pending_orders_tracker.items()):
        try:
            is_forex = order_info['is_forex']
            order_id = order_info['order_id']
            action = order_info['action']
            t_ep = order_info['entry']
            t_sl = order_info['sl']
            t_tp = order_info['tp']
            
            # Ambil data candle M15 terbaru
            if is_forex:
                df_ltf = fetch_ohlcv_forex(symbol, TIMEFRAME_LTF, limit=5)
            else:
                df_ltf = fetch_ohlcv_crypto(exchange, symbol, TIMEFRAME_LTF, limit=5)
                
            if df_ltf is None or len(df_ltf) == 0:
                continue
                
            latest_bar = df_ltf.iloc[-1]
            c_high, c_low, c_time = latest_bar['high'], latest_bar['low'], latest_bar['timestamp']
            
            # Tambahkan perhitungan bar jika candle M15 baru terbentuk
            if order_info.get('last_bar_time') != c_time:
                order_info['bars_count'] += 1
                order_info['last_bar_time'] = c_time
                
            b_cnt = order_info['bars_count']
            
            # 1. KASUS SELL PENDING
            if action == 'sell':
                if c_high >= t_ep:
                    # Order Terjemput -> Berubah jadi LIVE Position (Dikelola oleh exchange)
                    symbols_to_remove.append(symbol)
                    send_telegram(f"🟡 *[ORDER TERISI / LIVE]*\n• *Pair:* `{symbol}` (SELL)\n• *Entry:* `{t_ep:.5f}`")
                elif c_low <= t_tp:
                    # MISSED: Harga capai TP 1R duluan tanpa jemput entry
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, "MISSED ⚪ (TP 1R Hit First)")
                    symbols_to_remove.append(symbol)
                elif c_high >= t_sl:
                    # CANCELLED: Harga nembus SL duluan sebelum jemput entry
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, "CANCELLED ⚪ (SL Hit First)")
                    symbols_to_remove.append(symbol)
                elif b_cnt > MAX_PENDING_BARS:
                    # INVALID: Melewati batas 30 candle M15
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, f"INVALID ⚪ (Exceeded {MAX_PENDING_BARS} Bars)")
                    symbols_to_remove.append(symbol)

            # 2. KASUS BUY PENDING
            elif action == 'buy':
                if c_low <= t_ep:
                    # Order Terjemput -> Berubah jadi LIVE Position
                    symbols_to_remove.append(symbol)
                    send_telegram(f"🟡 *[ORDER TERISI / LIVE]*\n• *Pair:* `{symbol}` (BUY)\n• *Entry:* `{t_ep:.5f}`")
                elif c_high >= t_tp:
                    # MISSED: Harga capai TP 1R duluan tanpa jemput entry
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, "MISSED ⚪ (TP 1R Hit First)")
                    symbols_to_remove.append(symbol)
                elif c_low <= t_sl:
                    # CANCELLED: Harga nembus SL duluan sebelum jemput entry
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, "CANCELLED ⚪ (SL Hit First)")
                    symbols_to_remove.append(symbol)
                elif b_cnt > MAX_PENDING_BARS:
                    # INVALID: Melewati batas 30 candle M15
                    cancel_and_cleanup(exchange, symbol, order_id, is_forex, f"INVALID ⚪ (Exceeded {MAX_PENDING_BARS} Bars)")
                    symbols_to_remove.append(symbol)

        except Exception as e:
            print(f"Error pada manage_pending_orders [{symbol}]: {e}")

    # Bersihkan dari tracking lokal
    for sym in symbols_to_remove:
        if sym in pending_orders_tracker:
            del pending_orders_tracker[sym]

def cancel_and_cleanup(exchange, symbol, order_id, is_forex, reason):
    """Membatalkan order di Exchange/MT5 dan mengirim notifikasi"""
    try:
        if is_forex and MT5_AVAILABLE:
            request_data = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order_id
            }
            mt5.order_send(request_data)
        elif exchange:
            exchange.cancel_order(order_id, symbol)
            
        send_telegram(f"🗑 *[ORDER CANCELLED]*\n• *Pair:* `{symbol}`\n• *Status:* `{reason}`")
        log_trade(symbol, "CANCEL", 0, 0, 0, 0, 0, reason)
    except Exception as e:
        print(f"Gagal cancel order {symbol}: {e}")

# =================================================================
# ENGINE UTAMA: ALCHEMIST PRO HYBRID V2
# =================================================================
def process_asset(symbol, is_forex, exchange=None):
    global last_trade_times, IS_BOT_PAUSED, pending_orders_tracker
    
    if IS_BOT_PAUSED: return

    try:
        # Cek Batas Posisi + Floating Loss (Khusus Crypto)
        if not is_forex and exchange:
            can_trade, curr_pos, max_pos, free_usdt = check_dynamic_position_limit(exchange)
            if not can_trade: return

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

        # High & Low HTF (H1 Candle Sebelumnya / iloc[-2])
        h_htf = df_htf.iloc[-2]['high']
        l_htf = df_htf.iloc[-2]['low']

        # Hitung EMA 50 HTF (H1) & Daily Open
        df_htf['ema50'] = calculate_ema(df_htf['close'], 50)
        ema50_htf = df_htf.iloc[-2]['ema50']
        daily_open = df_daily.iloc[-1]['open']

        # Candle M15 Saat Ini
        current_bar = df_ltf.iloc[-1]
        c_close, c_high, c_low, c_open, c_time = current_bar['close'], current_bar['high'], current_bar['low'], current_bar['open'], current_bar['timestamp']

        # Filter Tren Alchemist (Sama Persis Pine Script)
        is_bull = (c_close > ema50_htf) and (c_close > daily_open)
        is_bear = (c_close < ema50_htf) and (c_close < daily_open)

        current_hour = datetime.now().hour
        in_kz = (LON_START <= current_hour < LON_END) or (NY_START <= current_hour < NY_END)

        s_cond = in_kz and (c_high > h_htf) and (c_close < h_htf) and (c_close < c_open) and is_bear
        b_cond = in_kz and (c_low < l_htf) and (c_close > l_htf) and (c_close > c_open) and is_bull

        last_time = last_trade_times.get(symbol)

        # Mencegah duplikasi sinyal & memastikan tidak ada pending order ganda di pair yang sama
        if (s_cond or b_cond) and (last_time != c_time) and (symbol not in pending_orders_tracker):
            action = 'sell' if s_cond else 'buy'
            entry_price = (c_high + c_low) / 2.0 if ENTRY_MODE == "retrace" else c_close

            if action == 'sell':
                sl_price = c_high + (SL_BUFFER_TICKS * tick_size)
                tp_price = entry_price - (sl_price - entry_price)  # TP 1R (100% Persis)
            else:
                sl_price = c_low - (SL_BUFFER_TICKS * tick_size)
                tp_price = entry_price + (entry_price - sl_price)  # TP 1R (100% Persis)

            order_res = None
            if is_forex:
                order_res = execute_forex_order(symbol, action, entry_price, sl_price, tp_price)
                order_id = order_res.order if order_res else None
                tipe_mkt = "FOREX"
                size_str = f"Lot {FOREX_LOT}"
            else:
                order_res = execute_crypto_order(exchange, symbol, action, entry_price, sl_price, tp_price)
                order_id = order_res.get('id') if order_res else None
                tipe_mkt = "CRYPTO"
                size_str = f"${CRYPTO_MARGIN_USDT} (Lev {CRYPTO_LEVERAGE}x)"

            if order_res:
                last_trade_times[symbol] = c_time
                
                # Masukkan ke Tracker Pending jika menggunakan mode Retrace 50%
                if ENTRY_MODE == "retrace" and order_id:
                    pending_orders_tracker[symbol] = {
                        'order_id': order_id,
                        'action': action,
                        'entry': entry_price,
                        'sl': sl_price,
                        'tp': tp_price,
                        'created_at_time': c_time,
                        'last_bar_time': c_time,
                        'bars_count': 0,
                        'is_forex': is_forex
                    }

                msg = (
                    f"🚀 *[ALCHEMIST AUTO-ENTRY - {tipe_mkt}]*\n\n"
                    f"• *Pair:* `{symbol}`\n"
                    f"• *Aksi:* `{action.upper()}` ({'Retrace 50%' if ENTRY_MODE == 'retrace' else 'Market'})\n"
                    f"• *Harga Entry:* `{entry_price:.5f}`\n"
                    f"• *Stop Loss:* `{sl_price:.5f}`\n"
                    f"• *Take Profit (1R):* `{tp_price:.5f}`\n"
                    f"• *Size:* `{size_str}`"
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
            if not IS_BOT_PAUSED:
                # 1. Kelola dan periksa Auto-Cancel pada Pending Limit Orders
                manage_pending_orders(exchange)
                
                # 2. Pindai Pasangan Mata Uang / Crypto
                crypto_pairs = get_top_crypto_pairs(exchange, TOP_CRYPTO_COUNT)
                
                if MT5_AVAILABLE and mt5.terminal_info():
                    for fx in FOREX_PAIRS:
                        process_asset(fx, is_forex=True)
                
                for coin in crypto_pairs:
                    process_asset(coin, is_forex=False, exchange=exchange)
                    time.sleep(0.1)

            time.sleep(15)
        except Exception as e:
            print(f"Error Scanner: {e}")
            time.sleep(15)

# =================================================================
# AUTO DAILY REPORT SCHEDULER
# =================================================================
def daily_report_scheduler():
    while True:
        now = datetime.now()
        if now.hour == 23 and now.minute == 59:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("SELECT status FROM trades WHERE created_at >= datetime('now', '-1 day')")
            rows = c.fetchall()
            conn.close()
            
            msg = (
                f"🌙 *[DAILY AUTO-REPORT]*\n\n"
                f"• *Tanggal:* `{now.strftime('%Y-%m-%d')}`\n"
                f"• *Total Trades Today:* `{len(rows)}x`\n\n"
                f"Gunakan `/stat` untuk rekap mingguan."
            )
            send_telegram(msg)
            time.sleep(70)
        time.sleep(30)

# =================================================================
# TELEGRAM COMMAND LISTENER
# =================================================================
@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    global IS_BOT_PAUSED
    update = request.get_json()
    if not update or 'message' not in update: return 'ok', 200

    msg_data = update['message']
    text = msg_data.get('text', '').strip()
    chat_id = str(msg_data['chat']['id'])

    if chat_id != TELEGRAM_CHAT_ID: return 'ok', 200

    if text == '/pause':
        IS_BOT_PAUSED = True
        send_telegram("⏸ *BOT DI-PAUSE!* Scanner dihentikan sementara.")

    elif text == '/resume':
        IS_BOT_PAUSED = False
        send_telegram("▶️ *BOT AKTIF KEMBALI!* Scanner mencari sinyal.")

    elif text == '/posisi':
        try:
            exchange = get_exchange()
            positions = exchange.fetch_positions()
            open_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]
            
            can_tr, curr_pos, max_pos, free_usdt = check_dynamic_position_limit(exchange)
            
            msg = f"📊 *STATUS POSISI & SALDO*\n\n"
            msg += f"• *Saldo Free USDT:* `${free_usdt:.2f}`\n"
            msg += f"• *Batas Max Posisi:* `{curr_pos} / {max_pos}`\n"
            msg += f"• *Pending Orders:* `{len(pending_orders_tracker)}`\n"
            msg += f"• *Status Bot:* `{'PAUSED ⏸' if IS_BOT_PAUSED else 'RUNNING ▶️'}`\n\n"
            
            if not open_pos:
                msg += "📭 Tidak ada posisi Crypto aktif saat ini."
            else:
                msg += "*Detail Posisi Running:*\n"
                for p in open_pos:
                    msg += f"• `{p['symbol']}` ({p.get('side', '').upper()}) | PnL: `{float(p.get('unrealizedPnl', 0)):+.2f} USDT`\n"
            
            send_telegram(msg)
        except Exception as e:
            send_telegram(f"❌ Gagal cek posisi: `{str(e)}`")

    elif text in ['/stat', '/mingguan']:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT status FROM trades WHERE created_at >= datetime('now', '-7 days')")
        rows = c.fetchall()
        conn.close()
        send_telegram(f"📈 *REKAP TRADING (7 HARI)*\n\n• *Total Signal Executed:* `{len(rows)}x`")

    return 'ok', 200

if __name__ == '__main__':
    init_db()
    
    scanner_thread = threading.Thread(target=multi_asset_scanner)
    scanner_thread.daemon = True
    scanner_thread.start()
    
    report_thread = threading.Thread(target=daily_report_scheduler)
    report_thread.daemon = True
    report_thread.start()
    
    app.run(host='0.0.0.0', port=5000)
