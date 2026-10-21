import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify

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
CORE_PAIRS = ['BTC/USDT', 'ETH/USDT']

DEFAULT_CEX = {
    'bingx': {'type': 'swap'},
    'bitget': {'type': 'swap'},
    'bybit': {'type': 'linear'},
    'binance': {'type': 'future'}
}

exchanges = {}

CONFIG = {
    'MARGIN_PER_TRADE': 2.0,
    'MAX_CONCURRENT_POSITIONS': 2,
    'LEVERAGE': {ex: 50 for ex in DEFAULT_CEX.keys()},
    'DRY_RUN': True,
    'MIN_TRADES': 5,     
    'MIN_WR': 40.0       
}

IS_BOT_PAUSED = False
daily_sl_count = 0
last_error_time = 0
ERROR_COOLDOWN = 300

# GLOBAL STATE UNTUK SETUP API
SETUP_STATE = {"status": "IDLE", "cex": "", "api": ""}

# =================================================================
# DATABASE & SETTINGS
# =================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, exchange_id TEXT, symbol TEXT, order_type TEXT, side TEXT, entry_price REAL, sl_price REAL, tp_price REAL, order_id TEXT, bars_count INTEGER DEFAULT 0, status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS cex_settings (ex_id TEXT PRIMARY KEY, api_key TEXT, secret TEXT, enabled INTEGER DEFAULT 0)''')
    try: c.execute("ALTER TABLE cex_settings ADD COLUMN enabled INTEGER DEFAULT 0")
    except: pass

    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('margin', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_pos', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('dry_run', 1.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_trades', 5.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_wr', 40.0)")
    for ex in DEFAULT_CEX.keys(): 
        c.execute(f"INSERT OR IGNORE INTO settings (key, value) VALUES ('lev_{ex}', 50.0)")
        c.execute("INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, enabled) VALUES (?, '', '', 0)", (ex,))
    conn.commit(); conn.close()

def load_settings():
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    for row in c.execute("SELECT key, value FROM settings").fetchall():
        if row[0] == 'margin': CONFIG['MARGIN_PER_TRADE'] = float(row[1])
        elif row[0] == 'max_pos': CONFIG['MAX_CONCURRENT_POSITIONS'] = int(row[1])
        elif row[0] == 'dry_run': CONFIG['DRY_RUN'] = bool(row[1])
        elif row[0] == 'min_trades': CONFIG['MIN_TRADES'] = int(row[1])
        elif row[0] == 'min_wr': CONFIG['MIN_WR'] = float(row[1])
        elif row[0].startswith('lev_'): CONFIG['LEVERAGE'][row[0].split('_')[1]] = int(row[1])
    conn.close()

def reload_exchanges():
    global exchanges
    conn = sqlite3.connect(DB_FILE)
    rows = conn.cursor().execute("SELECT ex_id, api_key, secret, enabled FROM cex_settings").fetchall()
    conn.close()
    exchanges.clear()
    for ex_id, api, sec, en in rows:
        if en == 1:
            try: 
                exchanges[ex_id] = getattr(ccxt, ex_id)({
                    'apiKey': api if api else '', 
                    'secret': sec if sec else '', 
                    'options': {'defaultType': DEFAULT_CEX[ex_id]['type']}, 
                    'enableRateLimit': True, 
                    'timeout': 5000
                })
            except: pass

def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    conn.cursor().execute("UPDATE settings SET value=? WHERE key=?", (value, key))
    conn.commit(); conn.close()

# =================================================================
# HELPERS (ANTI-BUNGKAM & AUTO DELETE)
# =================================================================
def send_telegram(message, reply_markup=None):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        if reply_markup: payload["reply_markup"] = reply_markup
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code != 200:
            payload.pop("parse_mode", None)
            requests.post(url, json=payload, timeout=5)
    except: pass

def delete_telegram_message(message_id):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id}, timeout=5)
    except: pass

def answer_callback(callback_id):
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery?callback_query_id={callback_id}", timeout=3)
    except: pass

def send_error_alert(err_msg):
    global last_error_time
    if time.time() - last_error_time > ERROR_COOLDOWN:
        send_telegram(f"⚠️ <b>ERROR ALERT</b>\n\n<code>{err_msg}</code>")
        last_error_time = time.time()

def is_kill_zone():
    h = datetime.now(WIB).hour
    return (14 <= h < 17) or (19 <= h < 22)

def is_manual_alert_symbol(symbol):
    return any(p in symbol.split(':')[0].upper() for p in MANUAL_ALERT_PAIRS)

def get_free_usdt_balance(ex_instance):
    try:
        bal = ex_instance.fetch_balance()
        usdt = bal.get('USDT', {})
        if isinstance(usdt, dict): return float(usdt.get('free', 0.0))
        return 0.0
    except: return 0.0

def cancel_all_pending_orders_on_pause():
    try:
        conn = sqlite3.connect(DB_FILE); c = conn.cursor()
        for t_id, ex_id, sym, ord_id in c.execute("SELECT id, exchange_id, symbol, order_id FROM trades WHERE status='PENDING'").fetchall():
            if ex_id in exchanges and not str(ord_id).startswith('sim_'):
                try: exchanges[ex_id].cancel_order(ord_id, sym)
                except: pass
            c.execute("UPDATE trades SET status='CANCELLED (Auto/Manual)' WHERE id=?", (t_id,))
        conn.commit(); conn.close()
        send_telegram("🧹 <b>CLEANUP</b> Semua pending order dibatalkan!")
    except: pass

def check_and_update_daily_sl():
    global daily_sl_count, IS_BOT_PAUSED
    try:
        conn = sqlite3.connect(DB_FILE)
        daily_sl_count = conn.cursor().execute("SELECT COUNT(*) FROM trades WHERE created_at >= date('now', 'start of day') AND status LIKE '%SL%'").fetchone()[0]
        conn.close()
        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True; cancel_all_pending_orders_on_pause()
            send_telegram(f"🛑 <b>DAILY LOSS LIMIT</b>\nKena {daily_sl_count}x SL. Bot PAUSE!")
    except: pass

# =================================================================
# SMC STRATEGY & LIFECYCLE
# =================================================================
def analyze_smc_retrace(df_m15, df_h1, daily_open):
    ema50 = df_h1['close'].ewm(span=50, adjust=False).mean().iloc[-2]
    h1_c, c_prev = df_h1['close'].iloc[-2], df_m15.iloc[-2]
    sig, en, sl, tp = None, 0.0, 0.0, 0.0
    if h1_c > ema50 and c_prev['close'] > daily_open:
        if c_prev['low'] < df_m15['low'].iloc[-7:-2].min() and c_prev['close'] > c_prev['open']:
            en = c_prev['high'] - (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['low'] * (1.0 - SL_BUFFER_PERCENT); tp = en + (en - sl); sig = 'BUY'
    elif h1_c < ema50 and c_prev['close'] < daily_open:
        if c_prev['high'] > df_m15['high'].iloc[-7:-2].max() and c_prev['close'] < c_prev['open']:
            en = c_prev['low'] + (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['high'] * (1.0 + SL_BUFFER_PERCENT); tp = en - (sl - en); sig = 'SELL'
    return sig, en, sl, tp

def update_orders_and_positions():
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    for t_id, ex_id, sym, side, en, sl, tp, ord_id, bars, status in c.execute("SELECT id, exchange_id, symbol, side, entry_price, sl_price, tp_price, order_id, bars_count, status FROM trades WHERE status IN ('PENDING', 'OPEN')").fetchall():
        if ex_id not in exchanges: continue
        try:
            curr_price = exchanges[ex_id].fetch_ticker(sym)['last']
            n_bars, is_sim = bars + 1, str(ord_id).startswith('sim_')
            tag = " (DRY RUN)" if is_sim else ""
            if status == 'PENDING':
                n_stat = 'PENDING'
                if (side == 'BUY' and curr_price <= en) or (side == 'SELL' and curr_price >= en): n_stat = 'OPEN'; send_telegram(f"🔔 <b>ORDER FILLED{tag}</b> {sym} di {ex_id.upper()}!")
                elif side == 'BUY':
                    if curr_price >= tp: n_stat = 'MISSED (TP First)'
                    elif curr_price <= sl: n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30: n_stat = 'INVALID (>30 Bars)'
                elif side == 'SELL':
                    if curr_price <= tp: n_stat = 'MISSED (TP First)'
                    elif curr_price >= sl: n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30: n_stat = 'INVALID (>30 Bars)'
                if n_stat != 'PENDING':
                    if ('CANCEL' in n_stat or 'MISS' in n_stat or 'INV' in n_stat) and not is_sim:
                        try: exchanges[ex_id].cancel_order(ord_id, sym)
                        except: pass
                    send_telegram(f"⚪ <b>AUTO-CANCEL{tag}</b> {sym}\nAlasan: {n_stat}")
                    c.execute("UPDATE trades SET status=?, bars_count=? WHERE id=?", (n_stat, n_bars, t_id))
                else: c.execute("UPDATE trades SET bars_count=? WHERE id=?", (n_bars, t_id))
            elif status == 'OPEN':
                if (side == 'BUY' and curr_price >= tp) or (side == 'SELL' and curr_price <= tp):
                    c.execute("UPDATE trades SET status='CLOSED (TP)' WHERE id=?", (t_id,))
                    send_telegram(f"🎉 <b>TAKE PROFIT{tag}</b> {sym} | +1R 🚀")
                elif (side == 'BUY' and curr_price <= sl) or (side == 'SELL' and curr_price >= sl):
                    c.execute("UPDATE trades SET status='CLOSED (SL)' WHERE id=?", (t_id,))
                    send_telegram(f"❌ <b>STOP LOSS{tag}</b> {sym} | -1R 🔻")
        except: pass
    conn.commit(); conn.close()

# =================================================================
# MARKET SCANNER
# =================================================================
def scan_markets():
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    act_count = c.execute("SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')").fetchone()[0]
    
    pair_perf = {}
    for sym, stat in c.execute("SELECT symbol, status FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')").fetchall():
        if sym not in pair_perf: pair_perf[sym] = {'w':0, 'l':0}
        if 'TP' in stat: pair_perf[sym]['w'] += 1
        else: pair_perf[sym]['l'] += 1
    conn.close()

    for ex_id, ex in exchanges.items():
        try:
            for sym in [s for s in ex.fetch_tickers().keys() if s.endswith('/USDT:USDT') or s.endswith('/USDT')][:60]:
                if is_manual_alert_symbol(sym):
                    m15, h1, d1 = ex.fetch_ohlcv(sym,'15m',limit=50), ex.fetch_ohlcv(sym,'1h',limit=60), ex.fetch_ohlcv(sym,'1d',limit=2)
                    if m15 and h1 and d1:
                        sig, en, sl, tp = analyze_smc_retrace(pd.DataFrame(m15, columns=['time','open','high','low','close','vol']), pd.DataFrame(h1, columns=['time','open','high','low','close','vol']), d1[-1][1])
                        if sig: send_telegram(f"📢 <b>FX ALERT</b> {sym} | {sig}")
                    continue

                if act_count >= CONFIG['MAX_CONCURRENT_POSITIONS']: continue

                if sym in pair_perf and sym not in CORE_PAIRS:
                    tot_trades = pair_perf[sym]['w'] + pair_perf[sym]['l']
                    if tot_trades >= CONFIG['MIN_TRADES']:
                        wr = (pair_perf[sym]['w'] / tot_trades) * 100
                        if wr < CONFIG['MIN_WR']: continue

                m15, h1, d1 = ex.fetch_ohlcv(sym,'15m',limit=50), ex.fetch_ohlcv(sym,'1h',limit=60), ex.fetch_ohlcv(sym,'1d',limit=2)
                if not m15 or not h1 or not d1: continue
                sig, en, sl, tp = analyze_smc_retrace(pd.DataFrame(m15, columns=['time','open','high','low','close','vol']), pd.DataFrame(h1, columns=['time','open','high','low','close','vol']), d1[-1][1])

                if sig:
                    lev = CONFIG['LEVERAGE'].get(ex_id, 50)
                    qty = (CONFIG['MARGIN_PER_TRADE'] * lev) / en
                    mode = "[DRY RUN]" if CONFIG['DRY_RUN'] else "[REAL TRADE]"
                    ord_id = f"sim_{int(time.time()*1000)}"

                    if not CONFIG['DRY_RUN']:
                        try: ex.set_leverage(lev, sym)
                        except: pass
                        try: ord_id = str(ex.create_order(sym, 'limit', 'buy' if sig=='BUY' else 'sell', qty, en, params={'stopLoss':{'price':sl},'takeProfit':{'price':tp}})['id'])
                        except Exception as e: send_error_alert(f"Order Fails {sym}: {e}"); continue

                    conn = sqlite3.connect(DB_FILE)
                    conn.cursor().execute("INSERT INTO trades (exchange_id, symbol, order_type, side, entry_price, sl_price, tp_price, order_id, status) VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')", (ex_id, sym, sig, en, sl, tp, ord_id))
                    conn.commit(); conn.close()

                    send_telegram(f"🎯 <b>{mode}</b>\nCEX: {ex_id.upper()} | {sym} | {sig}\nEntry: {en:.5f}")
                    act_count += 1
        except Exception as e: send_error_alert(f"Scan Ex [{ex_id}]: {e}")

# =================================================================
# TELEGRAM POLLING & INLINE KEYBOARDS
# =================================================================
def telegram_polling():
    global IS_BOT_PAUSED
    last_id = 0
    while True:
        try:
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_id+1}&timeout=5", timeout=10).json()
            if res.get("ok"):
                for r in res.get("result", []):
                    last_id = r["update_id"]
                    text, msg_id = "", ""
                    
                    if "callback_query" in r:
                        cb = r["callback_query"]
                        msg = cb.get("message", {})
                        if str(msg.get("chat", {}).get("id", "")) != str(TELEGRAM_CHAT_ID): continue
                        text = cb.get("data", "")
                        msg_id = msg.get("message_id")
                        answer_callback(cb["id"])
                    elif "message" in r:
                        msg = r["message"]
                        if str(msg.get("chat", {}).get("id", "")) != str(TELEGRAM_CHAT_ID): continue
                        text = msg.get("text", "").strip()
                        msg_id = msg.get("message_id")
                    else: continue
                    
                    parts = text.split(" ")

                    # -------------------------------------------------------------
                    # STATE MACHINE: INTERACTIVE API SETUP
                    # -------------------------------------------------------------
                    if SETUP_STATE["status"] == "AWAITING_API" and not text.startswith("/"):
                        delete_telegram_message(msg_id)
                        SETUP_STATE["api"] = text
                        SETUP_STATE["status"] = "AWAITING_SECRET"
                        send_telegram(f"🔐 <b>API Key diterima!</b>\n\nSekarang ketik dan kirimkan <b>Secret Key</b> untuk {SETUP_STATE['cex'].upper()}:")
                        continue
                        
                    elif SETUP_STATE["status"] == "AWAITING_SECRET" and not text.startswith("/"):
                        delete_telegram_message(msg_id)
                        api_key, secret_key, ex_id = SETUP_STATE["api"], text, SETUP_STATE["cex"]
                        SETUP_STATE["status"] = "IDLE"
                        send_telegram(f"⏳ <b>Memvalidasi Koneksi ke {ex_id.upper()}...</b>")
                        try:
                            test_ex = getattr(ccxt, ex_id)({'apiKey': api_key, 'secret': secret_key, 'options': {'defaultType': DEFAULT_CEX[ex_id]['type']}, 'enableRateLimit': False, 'timeout': 5000})
                            test_ex.fetch_balance()
                            conn = sqlite3.connect(DB_FILE)
                            conn.cursor().execute("UPDATE cex_settings SET api_key=?, secret=? WHERE ex_id=?", (api_key, secret_key, ex_id))
                            conn.commit(); conn.close(); reload_exchanges()
                            send_telegram(f"✅ <b>SUKSES!</b>\nAPI Key {ex_id.upper()} tervalidasi dan berhasil ditautkan.")
                        except Exception as e: send_telegram(f"❌ <b>API INVALID</b>\nGagal menghubungkan.\nError: <code>{e}</code>")
                        continue

                    # -------------------------------------------------------------
                    # NORMAL COMMANDS & MENUS
                    # -------------------------------------------------------------
                    if text.startswith("/"):
                        if not text.startswith("/setapi"): SETUP_STATE["status"] = "IDLE"
                            
                        # MENU UTAMA
                        if text in ["/start", "/help", "/menu"]:
                            keyboard = {
                                "inline_keyboard": [
                                    [{"text": "📊 Posisi", "callback_data": "/posisi"}, {"text": "⚙️ Info Setting", "callback_data": "/setting"}],
                                    [{"text": "🔑 Setup API", "callback_data": "/menu_api"}, {"text": "🔌 Nyalakan CEX", "callback_data": "/menu_cex"}],
                                    [{"text": "📈 Statistik", "callback_data": "/stat"}, {"text": "🏆 Performa", "callback_data": "/pairperf"}],
                                    [{"text": "🎚️ Set Lev/Margin", "callback_data": "/menu_margin"}, {"text": "🔄 Simulasi/Real", "callback_data": "/dryrun_toggle"}],
                                    [{"text": "⏸️ Pause/Resume Bot", "callback_data": "/pause_toggle"}]
                                ]
                            }
                            send_telegram("🤖 <b>ALCHEMIST CONTROL PANEL</b>\nPilih menu di bawah ini:", reply_markup=keyboard)
                            
                        # SUB-MENU API
                        elif text == "/menu_api":
                            kb = {"inline_keyboard": [
                                [{"text": "BingX", "callback_data": "/setapi bingx"}, {"text": "Bitget", "callback_data": "/setapi bitget"}],
                                [{"text": "Bybit", "callback_data": "/setapi bybit"}, {"text": "Binance", "callback_data": "/setapi binance"}]
                            ]}
                            send_telegram("🔑 <b>Pilih Bursa untuk Setup API:</b>", reply_markup=kb)

                        # SUB-MENU ON/OFF CEX
                        elif text == "/menu_cex":
                            kb = {"inline_keyboard": [
                                [{"text": "🟢 ON BingX", "callback_data": "/cex on bingx"}, {"text": "🔴 OFF BingX", "callback_data": "/cex off bingx"}],
                                [{"text": "🟢 ON Bitget", "callback_data": "/cex on bitget"}, {"text": "🔴 OFF Bitget", "callback_data": "/cex off bitget"}],
                                [{"text": "🟢 ON Bybit", "callback_data": "/cex on bybit"}, {"text": "🔴 OFF Bybit", "callback_data": "/cex off bybit"}],
                                [{"text": "🟢 ON Binance", "callback_data": "/cex on binance"}, {"text": "🔴 OFF Binance", "callback_data": "/cex off binance"}]
                            ]}
                            send_telegram("🔌 <b>Pilih Bursa yang mau dinyalakan/dimatikan:</b>", reply_markup=kb)

                        # SUB-MENU LEVERAGE & MARGIN
                        elif text == "/menu_margin":
                            msg_text = ("🎚️ <b>PENGATURAN MARGIN & LEVERAGE</b>\n\n"
                                   "Karena butuh angka spesifik, silakan balas dengan mengetik perintah ini:\n\n"
                                   "<b>1. Ubah Leverage:</b>\n"
                                   "<code>/setlev bingx 50</code> (khusus BingX)\n"
                                   "<code>/setlev 50</code> (semua bursa)\n\n"
                                   "<b>2. Ubah Margin per Trade:</b>\n"
                                   "<code>/setmargin 5.0</code> (artinya $5)\n\n"
                                   "<b>3. Ubah Max Posisi:</b>\n"
                                   "<code>/setmaxpos 3</code> (maksimal 3 posisi bersamaan)")
                            send_telegram(msg_text)

                        elif text.startswith("/setapi"):
                            if "callback_query" in r: delete_telegram_message(msg_id)
                            if len(parts) == 2:
                                ex_id = parts[1].lower()
                                if ex_id in DEFAULT_CEX:
                                    SETUP_STATE["status"] = "AWAITING_API"
                                    SETUP_STATE["cex"] = ex_id
                                    send_telegram(f"🔧 <b>SETUP API {ex_id.upper()}</b>\nKirimkan <b>API Key</b> kamu sekarang:")
                                else: send_telegram(f"⚠️ Pilihan: {', '.join(DEFAULT_CEX.keys())}")
                            else: send_telegram("⚠️ Gunakan format: <code>/setapi bingx</code>")
                        
                        elif text.startswith("/cex ") and len(parts) == 3:
                            if "callback_query" in r: delete_telegram_message(msg_id)
                            action, ex_id = parts[1].lower(), parts[2].lower()
                            if ex_id in DEFAULT_CEX and action in ['on', 'off']:
                                conn = sqlite3.connect(DB_FILE)
                                conn.cursor().execute("UPDATE cex_settings SET enabled=? WHERE ex_id=?", (1 if action=='on' else 0, ex_id))
                                conn.commit(); conn.close(); reload_exchanges()
                                send_telegram(f"✅ CEX {ex_id.upper()} di-{action.upper()}!")

                        elif text == "/dryrun_toggle": 
                            CONFIG['DRY_RUN'] = not CONFIG['DRY_RUN']
                            update_db_setting('dry_run', 1.0 if CONFIG['DRY_RUN'] else 0.0)
                            send_telegram("✅ <b>DRY RUN AKTIF</b>" if CONFIG['DRY_RUN'] else "⚠️ <b>REAL MONEY AKTIF</b>")

                        elif text.startswith("/setmargin ") and len(parts)==2: CONFIG['MARGIN_PER_TRADE']=float(parts[1]); update_db_setting('margin',float(parts[1])); send_telegram(f"✅ Margin: ${parts[1]}")
                        elif text.startswith("/setmaxpos ") and len(parts)==2: CONFIG['MAX_CONCURRENT_POSITIONS']=int(parts[1]); update_db_setting('max_pos',int(parts[1])); send_telegram(f"✅ Max Posisi: {parts[1]}")
                        elif text.startswith("/setminwr ") and len(parts)==2: CONFIG['MIN_WR']=float(parts[1]); update_db_setting('min_wr',float(parts[1])); send_telegram(f"✅ Min WinRate: {parts[1]}%")
                        elif text.startswith("/setmintrades ") and len(parts)==2: CONFIG['MIN_TRADES']=int(parts[1]); update_db_setting('min_trades',int(parts[1])); send_telegram(f"✅ Min Trade: {parts[1]}x")
                        elif text.startswith("/setlev "):
                            if len(parts) == 2:
                                for ex in DEFAULT_CEX.keys(): CONFIG['LEVERAGE'][ex]=int(parts[1]); update_db_setting(f'lev_{ex}',int(parts[1]))
                                send_telegram(f"✅ Semua leverage: {parts[1]}x")
                            elif len(parts) == 3 and parts[1].lower() in DEFAULT_CEX:
                                CONFIG['LEVERAGE'][parts[1].lower()]=int(parts[2]); update_db_setting(f'lev_{parts[1].lower()}',int(parts[2])); send_telegram(f"✅ Lev {parts[1].upper()}: {parts[2]}x")
                        
                        elif text == "/pairperf":
                            conn = sqlite3.connect(DB_FILE); c = conn.cursor()
                            pair_perf = {}
                            for sym, stat in c.execute("SELECT symbol, status FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')").fetchall():
                                if sym not in pair_perf: pair_perf[sym] = {'w':0, 'l':0}
                                if 'TP' in stat: pair_perf[sym]['w'] += 1
                                else: pair_perf[sym]['l'] += 1
                            conn.close()
                            gacor, blist, eval_wait = "", "", ""
                            for sym, data in pair_perf.items():
                                t = data['w'] + data['l']
                                wr = (data['w'] / t) * 100
                                is_core = sym in CORE_PAIRS
                                core_tag = " [CORE]" if is_core else ""
                                line = f"• {sym}{core_tag}: {data['w']}W/{data['l']}L (WR: {wr:.1f}%)\n"
                                if t < CONFIG['MIN_TRADES']: eval_wait += line
                                elif is_core or wr >= CONFIG['MIN_WR']: gacor += line
                                else: blist += line
                            msg = f"🏆 <b>PERFORMA KOIN</b>\n\n🔥 <b>AKTIF</b>\n{gacor or 'Belum ada'}\n\n⏳ <b>EVALUASI</b>\n{eval_wait or 'Belum ada'}\n\n🗑️ <b>BLACKLIST</b>\n{blist or 'Belum ada'}"
                            send_telegram(msg)
                            
                        elif text in ["/setting", "/settings"]:
                            try:
                                conn = sqlite3.connect(DB_FILE)
                                cex_db = conn.cursor().execute("SELECT ex_id, api_key, enabled FROM cex_settings").fetchall()
                                conn.close()
                                mode_text = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
                                cex_info = "".join([f"• {ex.upper()}: {'🟢 ON' if en else '🔴 OFF'} | Lev: {CONFIG['LEVERAGE'].get(ex, 50)}x | API: {'✅' if api else '❌'}\n" for ex, api, en in cex_db])
                                msg = f"⚙️ <b>SETTING BOT</b>\n\nMode: {mode_text}\nMargin: ${CONFIG['MARGIN_PER_TRADE']}\nMax Pos: {CONFIG['MAX_CONCURRENT_POSITIONS']}\n\n<b>BURSA:</b>\n{cex_info or 'Belum ada'}"
                                send_telegram(msg)
                            except Exception as e: send_telegram(f"⚠️ ERROR: {e}")
                        
                        elif text == "/posisi":
                            try:
                                conn = sqlite3.connect(DB_FILE); c = conn.cursor()
                                trades = c.execute("SELECT exchange_id, symbol, side, status, order_id FROM trades WHERE status IN ('PENDING', 'OPEN')").fetchall()
                                cex_db = c.execute("SELECT ex_id, api_key, enabled FROM cex_settings").fetchall()
                                conn.close()
                                
                                cex_stat = "".join([f"• {ex.upper()} {'🟢 ON' if en else '🔴 OFF'}\n" for ex, api, en in cex_db])
                                bal = ""
                                for k, v in exchanges.items():
                                    try:
                                        b = get_free_usdt_balance(v)
                                        bal += f"• {k.upper()} Free: ${b:.2f}\n"
                                    except: bal += f"• {k.upper()} Free: Error/Timeout\n"
                                if not bal: bal = "• Saldo: Tidak ada CEX aktif"

                                mode = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL"
                                m = f"📊 <b>STATUS & POSISI</b>\nBot: {'PAUSED' if IS_BOT_PAUSED else 'RUNNING'} | Mode: {mode}\n\n🔌 <b>CEX:</b>\n{cex_stat}\n💰 <b>SALDO:</b>\n{bal}\n📋 <b>ANTREAN:</b> {len(trades)}\n"
                                for t in trades: m += f"• {t[0].upper()} | {t[1]} | {t[2]} | {t[3]}\n"
                                send_telegram(m)
                            except Exception as e: send_telegram(f"⚠️ ERROR POSISI: {e}")

                        elif text == "/pause_toggle":
                            IS_BOT_PAUSED = not IS_BOT_PAUSED
                            status = "⏸️ PAUSED" if IS_BOT_PAUSED else "▶️ RESUMED"
                            send_telegram(f"Sistem sekarang: <b>{status}</b>")
                        
                        elif text == "/stat":
                            conn = sqlite3.connect(DB_FILE)
                            stats = "\n".join([f"• {s[0]}: {s[1]}" for s in conn.cursor().execute("SELECT status, COUNT(*) FROM trades GROUP BY status").fetchall()])
                            conn.close()
                            send_telegram(f"📈 <b>STATISTIK</b>\n\n{stats or 'Belum ada data'}")
        except: time.sleep(3)
        time.sleep(1)

# =================================================================
# WEB DASHBOARD & FLASK SERVER
# =================================================================
app = Flask(__name__, template_folder='templates')

@app.route('/')
def dashboard(): return render_template('index.html')

@app.route('/api/stats')
def api_stats():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT symbol, side, status, created_at FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)') ORDER BY id ASC", conn)
    conn.close()
    if df.empty: return jsonify({'dates': [], 'pnl': [], 'win_rate': 0, 'wins': 0, 'losses': 0, 'net_pnl': 0, 'recent': []})
    margin = CONFIG.get('MARGIN_PER_TRADE', 2.0)
    df['pnl_value'] = df['status'].apply(lambda x: margin if 'TP' in x else -margin)
    df['cum_pnl'] = df['pnl_value'].cumsum()
    df['date_only'] = pd.to_datetime(df['created_at']).dt.strftime('%d %b %H:%M')
    wins, losses = len(df[df['status'] == 'CLOSED (TP)']), len(df[df['status'] == 'CLOSED (SL)'])
    total = wins + losses
    win_rate = round((wins / total) * 100, 1) if total > 0 else 0
    net_pnl = round(df['cum_pnl'].iloc[-1], 2) if not df.empty else 0
    recent = df.tail(10)[::-1].to_dict('records')
    return jsonify({'dates': df['date_only'].tolist(), 'pnl': df['cum_pnl'].tolist(), 'win_rate': win_rate, 'wins': wins, 'losses': losses, 'net_pnl': net_pnl, 'recent': recent})

def main_loop():
    last_hb = time.time()
    send_telegram("🚀 <b>ALCHEMIST v2 INITIALIZED</b>")
    while True:
        try:
            now = datetime.now(WIB)
            if now.hour == 7 and now.minute == 0 and now.second < 15:
                global daily_sl_count, IS_BOT_PAUSED
                daily_sl_count = 0
                if IS_BOT_PAUSED: IS_BOT_PAUSED = False; send_telegram("🌅 <b>NEW DAY RESET</b>")
            if time.time() - last_hb >= 21600: send_telegram("💓 <b>PING</b> Sistem normal."); last_hb = time.time()
            check_and_update_daily_sl()
            update_orders_and_positions()
            if not IS_BOT_PAUSED and is_kill_zone(): scan_markets()
        except Exception as e: send_error_alert(str(e))
        time.sleep(15)

if __name__ == '__main__':
    init_db(); load_settings(); reload_exchanges()
    threading.Thread(target=telegram_polling, daemon=True).start()
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000), daemon=True).start()
    main_loop()
