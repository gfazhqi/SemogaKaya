import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
from datetime import datetime, timedelta, timezone
from flask import Flask

# =================================================================
# CONFIGURATION & PARAMETERS
# =================================================================
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

SL_BUFFER_PERCENT = 0.0005
MAX_DAILY_SL = 2
DB_FILE = "trades.db"
WIB = timezone(timedelta(hours=7))

MANUAL_ALERT_PAIRS = ['XAU/USDT', 'EUR/USDT', 'GBP/USDT', 'USOIL/USDT', 'WTI/USDT', 'USO/USDT']

# =================================================================
# MULTI-CEX ARCHITECTURE ENGINE
# =================================================================
EXCHANGE_CONFIGS = {
    'bingx': {'enabled': True, 'apiKey': 'YOUR_BINGX_API_KEY', 'secret': 'YOUR_BINGX_SECRET_KEY', 'options': {'defaultType': 'swap'}, 'enableRateLimit': True},
    'bitget': {'enabled': False, 'apiKey': 'YOUR_BITGET_API_KEY', 'secret': 'YOUR_BITGET_SECRET_KEY', 'options': {'defaultType': 'swap'}, 'enableRateLimit': True},
    'bybit': {'enabled': False, 'apiKey': 'YOUR_BYBIT_API_KEY', 'secret': 'YOUR_BYBIT_SECRET_KEY', 'options': {'defaultType': 'linear'}, 'enableRateLimit': True},
    'binance': {'enabled': False, 'apiKey': 'YOUR_BINANCE_API_KEY', 'secret': 'YOUR_BINANCE_SECRET_KEY', 'options': {'defaultType': 'future'}, 'enableRateLimit': True}
}

exchanges = {}
for ex_id, cfg in EXCHANGE_CONFIGS.items():
    if cfg['enabled']:
        ex_class = getattr(ccxt, ex_id)
        exchanges[ex_id] = ex_class({
            'apiKey': cfg['apiKey'], 'secret': cfg['secret'],
            'options': cfg['options'], 'enableRateLimit': cfg['enableRateLimit']
        })

# Global Dynamic Settings
CONFIG = {
    'MARGIN_PER_TRADE': 2.0,
    'MAX_CONCURRENT_POSITIONS': 2,
    'LEVERAGE': {ex: 50 for ex in EXCHANGE_CONFIGS.keys()} # Default 50x per CEX
}

IS_BOT_PAUSED = False
daily_sl_count = 0
last_error_time = 0
ERROR_COOLDOWN = 300

# =================================================================
# DATABASE & DYNAMIC SETTINGS INITIALIZATION
# =================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, exchange_id TEXT, symbol TEXT, order_type TEXT, side TEXT, entry_price REAL, sl_price REAL, tp_price REAL, order_id TEXT, bars_count INTEGER DEFAULT 0, status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value REAL)''')
    
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('margin', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_pos', 2.0)")
    for ex in EXCHANGE_CONFIGS.keys():
        c.execute(f"INSERT OR IGNORE INTO settings (key, value) VALUES ('lev_{ex}', 50.0)")
    conn.commit()
    conn.close()

def load_settings():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT key, value FROM settings")
    rows = c.fetchall()
    conn.close()
    
    for row in rows:
        if row[0] == 'margin': CONFIG['MARGIN_PER_TRADE'] = float(row[1])
        elif row[0] == 'max_pos': CONFIG['MAX_CONCURRENT_POSITIONS'] = int(row[1])
        elif row[0].startswith('lev_'):
            ex_name = row[0].split('_')[1]
            CONFIG['LEVERAGE'][ex_name] = int(row[1])

def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE settings SET value=? WHERE key=?", (value, key))
    conn.commit()
    conn.close()

# =================================================================
# TELEGRAM & HELPERS
# =================================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try: requests.post(url, json=payload, timeout=5)
    except: pass

def send_error_alert(err_msg):
    global last_error_time
    now = time.time()
    if now - last_error_time > ERROR_COOLDOWN:
        send_telegram(f"⚠️ *[ERROR ALERT GUARD]*\n\n`{err_msg}`")
        last_error_time = now

def is_kill_zone():
    now_wib = datetime.now(WIB)
    hour = now_wib.hour
    return (14 <= hour < 17) or (19 <= hour < 22)

def is_manual_alert_symbol(symbol):
    clean_symbol = symbol.split(':')[0].upper()
    return any(p in clean_symbol for p in MANUAL_ALERT_PAIRS)

def get_free_usdt_balance(ex_instance):
    try: return float(ex_instance.fetch_balance().get('USDT', {}).get('free', 0.0))
    except: return 0.0

def cancel_all_pending_orders_on_pause():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id, exchange_id, symbol, order_id FROM trades WHERE status='PENDING'")
        for row in c.fetchall():
            trade_id, ex_id, symbol, order_id = row
            if ex_id in exchanges:
                try:
                    exchanges[ex_id].cancel_order(order_id, symbol)
                    c.execute("UPDATE trades SET status='CANCELLED (Manual/Auto-Pause)' WHERE id=?", (trade_id,))
                except: pass
        conn.commit()
        conn.close()
        send_telegram("🧹 *[CLEANUP]* Semua pending order dibatalkan!")
    except: pass

def check_and_update_daily_sl():
    global daily_sl_count, IS_BOT_PAUSED
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM trades WHERE created_at >= date('now', 'start of day') AND status LIKE '%SL%'")
        daily_sl_count = c.fetchone()[0]
        conn.close()
        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True
            cancel_all_pending_orders_on_pause()
            send_telegram(f"🛑 *[DAILY LOSS LIMIT]*\nHari ini kena `{daily_sl_count}x SL`.\nBot di-*PAUSE*. Aktif besok 07:00 WIB!")
    except: pass

# =================================================================
# SMC STRATEGY
# =================================================================
def analyze_smc_retrace(df_m15, df_h1, daily_open):
    ema50_h1 = df_h1['close'].ewm(span=50, adjust=False).mean().iloc[-2]
    h1_close, c_prev = df_h1['close'].iloc[-2], df_m15.iloc[-2]
    signal, entry_price, sl_price, tp_price = None, 0.0, 0.0, 0.0

    if h1_close > ema50_h1 and c_prev['close'] > daily_open:
        min_low = df_m15['low'].iloc[-7:-2].min()
        if c_prev['low'] < min_low and c_prev['close'] > c_prev['open']:
            entry_price = c_prev['high'] - (0.5 * (c_prev['high'] - c_prev['low']))
            sl_price = c_prev['low'] * (1.0 - SL_BUFFER_PERCENT)
            tp_price = entry_price + (entry_price - sl_price)
            signal = 'BUY'
    elif h1_close < ema50_h1 and c_prev['close'] < daily_open:
        max_high = df_m15['high'].iloc[-7:-2].max()
        if c_prev['high'] > max_high and c_prev['close'] < c_prev['open']:
            entry_price = c_prev['low'] + (0.5 * (c_prev['high'] - c_prev['low']))
            sl_price = c_prev['high'] * (1.0 + SL_BUFFER_PERCENT)
            tp_price = entry_price - (sl_price - entry_price)
            signal = 'SELL'
    return signal, entry_price, sl_price, tp_price

# =================================================================
# ORDER LIFECYCLE
# =================================================================
def update_orders_and_positions():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, exchange_id, symbol, side, entry_price, sl_price, tp_price, order_id, bars_count, status FROM trades WHERE status IN ('PENDING', 'OPEN')")
    
    for trade_id, ex_id, symbol, side, entry, sl, tp, order_id, bars, status in c.fetchall():
        if ex_id not in exchanges: continue
        try:
            ex_instance = exchanges[ex_id]
            curr_price = ex_instance.fetch_ticker(symbol)['last']
            new_bars = bars + 1

            if status == 'PENDING':
                new_status = 'PENDING'
                if (side == 'BUY' and curr_price <= entry) or (side == 'SELL' and curr_price >= entry):
                    new_status = 'OPEN'
                    send_telegram(f"🔔 *[ORDER FILLED]*\nPair: `{symbol}`\nPosisi *LIVE* di {ex_id.upper()}!")
                elif side == 'BUY':
                    if curr_price >= tp: new_status = 'MISSED (TP First)'
                    elif curr_price <= sl: new_status = 'CANCELLED (SL First)'
                    elif new_bars > 30: new_status = 'INVALID (>30 Bars)'
                elif side == 'SELL':
                    if curr_price <= tp: new_status = 'MISSED (TP First)'
                    elif curr_price >= sl: new_status = 'CANCELLED (SL First)'
                    elif new_bars > 30: new_status = 'INVALID (>30 Bars)'

                if new_status != 'PENDING':
                    if 'CANCEL' in new_status or 'MISSED' in new_status or 'INVALID' in new_status:
                        try: ex_instance.cancel_order(order_id, symbol)
                        except: pass
                        send_telegram(f"⚪ *[AUTO-CANCEL]*\nPair: `{symbol}`\nAlasan: `{new_status}`")
                    c.execute("UPDATE trades SET status=?, bars_count=? WHERE id=?", (new_status, new_bars, trade_id))
                else: c.execute("UPDATE trades SET bars_count=? WHERE id=?", (new_bars, trade_id))

            elif status == 'OPEN':
                if (side == 'BUY' and curr_price >= tp) or (side == 'SELL' and curr_price <= tp):
                    c.execute("UPDATE trades SET status='CLOSED (TP)' WHERE id=?", (trade_id,))
                    send_telegram(f"🎉 *[TAKE PROFIT HIT]*\nPair: `{symbol}`\nProfit: `+1R` 🚀")
                elif (side == 'BUY' and curr_price <= sl) or (side == 'SELL' and curr_price >= sl):
                    c.execute("UPDATE trades SET status='CLOSED (SL)' WHERE id=?", (trade_id,))
                    send_telegram(f"❌ *[STOP LOSS HIT]*\nPair: `{symbol}`\nLoss: `-1R` 🔻")
        except: pass
    conn.commit()
    conn.close()

# =================================================================
# MARKET SCANNER ENGINE
# =================================================================
def scan_markets():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')")
    active_count = c.fetchone()[0]
    conn.close()

    for ex_id, ex_instance in exchanges.items():
        try:
            tickers = ex_instance.fetch_tickers()
            all_symbols = [s for s in tickers.keys() if s.endswith('/USDT:USDT') or s.endswith('/USDT')][:60]

            for symbol in all_symbols:
                if is_manual_alert_symbol(symbol):
                    ohlcv_m15 = ex_instance.fetch_ohlcv(symbol, '15m', limit=50)
                    ohlcv_h1 = ex_instance.fetch_ohlcv(symbol, '1h', limit=60)
                    ohlcv_d1 = ex_instance.fetch_ohlcv(symbol, '1d', limit=2)
                    if not ohlcv_m15 or not ohlcv_h1 or not ohlcv_d1: continue
                    signal, entry, sl, tp = analyze_smc_retrace(pd.DataFrame(ohlcv_m15, columns=['time','open','high','low','close','vol']), pd.DataFrame(ohlcv_h1, columns=['time','open','high','low','close','vol']), ohlcv_d1[-1][1])
                    if signal: send_telegram(f"📢 *[FX ALERT]* `{symbol}` | `{signal}`\nEntry: `{entry:.5f}` | TP: `{tp:.5f}`")
                    continue

                if active_count >= CONFIG['MAX_CONCURRENT_POSITIONS']: continue

                ohlcv_m15 = ex_instance.fetch_ohlcv(symbol, '15m', limit=50)
                ohlcv_h1 = ex_instance.fetch_ohlcv(symbol, '1h', limit=60)
                ohlcv_d1 = ex_instance.fetch_ohlcv(symbol, '1d', limit=2)
                if not ohlcv_m15 or not ohlcv_h1 or not ohlcv_d1: continue

                signal, entry, sl, tp = analyze_smc_retrace(pd.DataFrame(ohlcv_m15, columns=['time','open','high','low','close','vol']), pd.DataFrame(ohlcv_h1, columns=['time','open','high','low','close','vol']), ohlcv_d1[-1][1])

                if signal:
                    # Ambil settingan leverage khusus untuk CEX ini
                    current_lev = CONFIG['LEVERAGE'].get(ex_id, 50)
                    try: ex_instance.set_leverage(current_lev, symbol)
                    except: pass

                    qty = (CONFIG['MARGIN_PER_TRADE'] * current_lev) / entry
                    order = ex_instance.create_order(symbol=symbol, type='limit', side='buy' if signal=='BUY' else 'sell', amount=qty, price=entry, params={'stopLoss': {'price': sl}, 'takeProfit': {'price': tp}})

                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute("INSERT INTO trades (exchange_id, symbol, order_type, side, entry_price, sl_price, tp_price, order_id, status) VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')", (ex_id, symbol, signal, entry, sl, tp, str(order['id'])))
                    conn.commit()
                    conn.close()

                    send_telegram(f"🎯 *[NEW CRYPTO SIGNAL]*\nCEX: `{ex_id.upper()}` | Pair: `{symbol}` | `{signal}`\nEntry: `{entry:.5f}`\nMargin: `${CONFIG['MARGIN_PER_TRADE']}` ({current_lev}x)")
                    active_count += 1
        except Exception as e: send_error_alert(f"Scan Exception [{ex_id}]: {e}")

# =================================================================
# TELEGRAM COMMANDS
# =================================================================
def telegram_polling():
    last_update_id = 0
    while True:
        try:
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10", timeout=15).json()
            if res.get("ok"):
                for result in res.get("result", []):
                    last_update_id = result["update_id"]
                    msg_data = result.get("message", {})
                    if str(msg_data.get("chat", {}).get("id", "")) != str(TELEGRAM_CHAT_ID): continue
                    
                    text = msg_data.get("text", "").strip()
                    parts = text.split(" ")

                    if text.startswith("/setmargin ") and len(parts) == 2:
                        try:
                            val = float(parts[1])
                            CONFIG['MARGIN_PER_TRADE'] = val
                            update_db_setting('margin', val)
                            send_telegram(f"✅ Margin diubah: `${val}`")
                        except: send_telegram("❌ Format salah! Contoh: `/setmargin 5`")

                    elif text.startswith("/setmaxpos ") and len(parts) == 2:
                        try:
                            val = int(parts[1])
                            CONFIG['MAX_CONCURRENT_POSITIONS'] = val
                            update_db_setting('max_pos', val)
                            send_telegram(f"✅ Max Posisi diubah: `{val}`")
                        except: send_telegram("❌ Format salah! Contoh: `/setmaxpos 4`")

                    elif text.startswith("/setlev "):
                        try:
                            if len(parts) == 2: # Global: /setlev 50
                                val = int(parts[1])
                                for ex in EXCHANGE_CONFIGS.keys():
                                    CONFIG['LEVERAGE'][ex] = val
                                    update_db_setting(f'lev_{ex}', val)
                                send_telegram(f"✅ Leverage SEMUA bursa diubah jadi `{val}x`")
                            elif len(parts) == 3: # Per CEX: /setlev bingx 150
                                ex_name, val = parts[1].lower(), int(parts[2])
                                if ex_name in EXCHANGE_CONFIGS:
                                    CONFIG['LEVERAGE'][ex_name] = val
                                    update_db_setting(f'lev_{ex_name}', val)
                                    send_telegram(f"✅ Leverage {ex_name.upper()} diubah jadi `{val}x`")
                                else: send_telegram(f"❌ Bursa {ex_name} tidak ditemukan!")
                            else: raise ValueError
                        except: send_telegram("❌ Contoh:\nSemua: `/setlev 50`\nPer CEX: `/setlev bingx 150`")

                    elif text in ["/start", "/help"]:
                        send_telegram("🤖 *[ALCHEMIST COMMANDS]*\n• `/posisi` - Status & Saldo\n• `/setmargin <angka>` - Margin USDT\n• `/setlev <cex> <angka>` - Ubah Leverage\n• `/setmaxpos <angka>` - Max Posisi\n• `/pause` | `/resume`\n• `/cancelall` | `/stat`")

                    elif text == "/posisi":
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute("SELECT exchange_id, symbol, side, status FROM trades WHERE status IN ('PENDING', 'OPEN')")
                        trades = c.fetchall()
                        conn.close()

                        bal_info = "\n".join([f"• `{k.upper()}` Free: `${get_free_usdt_balance(v):.2f}`" for k, v in exchanges.items()])
                        lev_info = "\n".join([f"• {k.upper()}: `{v}x`" for k,v in CONFIG['LEVERAGE'].items() if EXCHANGE_CONFIGS[k]['enabled']])
                        
                        msg = (f"📊 *[STATUS & POSISI]*\nBot: `{'PAUSED 🛑' if IS_BOT_PAUSED else 'RUNNING 🟢'}`\nSL Hari Ini: `{daily_sl_count}/{MAX_DAILY_SL}`\n\n"
                               f"⚙️ *Setting:*\n• Margin: `${CONFIG['MARGIN_PER_TRADE']}`\n• Max Pos: `{CONFIG['MAX_CONCURRENT_POSITIONS']}`\n*Leverage:*\n{lev_info}\n\n"
                               f"💰 *Saldo:*\n{bal_info}\n\n📋 *Antrean:* {len(trades)}\n")
                        for t in trades: msg += f"• `[{t[0].upper()}]` `{t[1]}` | `{t[2]}` | `{t[3]}`\n"
                        send_telegram(msg)

                    elif text == "/pause": IS_BOT_PAUSED = True; send_telegram("⏸️ Bot PAUSED")
                    elif text == "/resume": IS_BOT_PAUSED = False; send_telegram("▶️ Bot RESUMED")
                    elif text == "/cancelall": cancel_all_pending_orders_on_pause()
                    elif text == "/stat":
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute("SELECT status, COUNT(*) FROM trades GROUP BY status")
                        stats = "\n".join([f"• `{s[0]}`: {s[1]}" for s in c.fetchall()])
                        conn.close()
                        send_telegram(f"📈 *[STATISTIK]*\n\n{stats}")
        except: time.sleep(5)
        time.sleep(1)

# =================================================================
# SERVER & MAIN LOOP
# =================================================================
app = Flask(__name__)
@app.route('/')
def home(): return "Alchemist Pro v2 Running!"
def run_flask(): app.run(host='0.0.0.0', port=5000)

def main_loop():
    last_hb = time.time()
    send_telegram("🚀 *[ALCHEMIST v2 INITIALIZED]*")
    while True:
        try:
            now = datetime.now(WIB)
            if now.hour == 7 and now.minute == 0 and now.second < 15:
                global daily_sl_count, IS_BOT_PAUSED
                daily_sl_count = 0
                if IS_BOT_PAUSED: IS_BOT_PAUSED = False; send_telegram("🌅 *[NEW DAY RESET]* Bot kembali aktif!")
            if time.time() - last_hb >= 21600: send_telegram("💓 *[PING]* Sistem normal."); last_hb = time.time()

            check_and_update_daily_sl()
            update_orders_and_positions()
            if not IS_BOT_PAUSED and is_kill_zone(): scan_markets()
        except Exception as e: send_error_alert(str(e))
        time.sleep(15)

if __name__ == '__main__':
    init_db()
    load_settings()
    threading.Thread(target=telegram_polling, daemon=True).start()
    threading.Thread(target=run_flask, daemon=True).start()
    main_loop()
