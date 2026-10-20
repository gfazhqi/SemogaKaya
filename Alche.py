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
    'DRY_RUN': True  # Default Simulasi
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
    c.execute('''CREATE TABLE IF NOT EXISTS cex_settings (ex_id TEXT PRIMARY KEY, api_key TEXT, secret TEXT, enabled INTEGER)''')
    
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('margin', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_pos', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('dry_run', 1.0)")
    for ex in DEFAULT_CEX.keys(): 
        c.execute(f"INSERT OR IGNORE INTO settings (key, value) VALUES ('lev_{ex}', 50.0)")
        c.execute("INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, enabled) VALUES (?, '', '', 0)", (ex,))
    conn.commit(); conn.close()

def load_settings():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for row in c.execute("SELECT key, value FROM settings").fetchall():
        if row[0] == 'margin': CONFIG['MARGIN_PER_TRADE'] = float(row[1])
        elif row[0] == 'max_pos': CONFIG['MAX_CONCURRENT_POSITIONS'] = int(row[1])
        elif row[0] == 'dry_run': CONFIG['DRY_RUN'] = bool(row[1])
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
                ex_class = getattr(ccxt, ex_id)
                exchanges[ex_id] = ex_class({
                    'apiKey': api if api else '', 'secret': sec if sec else '',
                    'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
                    'enableRateLimit': True
                })
            except: pass

def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    conn.cursor().execute("UPDATE settings SET value=? WHERE key=?", (value, key))
    conn.commit(); conn.close()

# =================================================================
# HELPERS
# =================================================================
def send_telegram(message):
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except: pass

def send_error_alert(err_msg):
    global last_error_time
    if time.time() - last_error_time > ERROR_COOLDOWN:
        send_telegram(f"⚠️ *[ERROR ALERT]*\n\n`{err_msg}`")
        last_error_time = time.time()

def is_kill_zone():
    h = datetime.now(WIB).hour
    return (14 <= h < 17) or (19 <= h < 22)

def is_manual_alert_symbol(symbol):
    return any(p in symbol.split(':')[0].upper() for p in MANUAL_ALERT_PAIRS)

def get_free_usdt_balance(ex_instance):
    try: return float(ex_instance.fetch_balance().get('USDT', {}).get('free', 0.0))
    except: return 0.0

def cancel_all_pending_orders_on_pause():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        for t_id, ex_id, sym, ord_id in c.execute("SELECT id, exchange_id, symbol, order_id FROM trades WHERE status='PENDING'").fetchall():
            if ex_id in exchanges and not str(ord_id).startswith('sim_'):
                try: exchanges[ex_id].cancel_order(ord_id, sym)
                except: pass
            c.execute("UPDATE trades SET status='CANCELLED (Auto/Manual)' WHERE id=?", (t_id,))
        conn.commit(); conn.close()
        send_telegram("🧹 *[CLEANUP]* Semua pending order dibatalkan!")
    except: pass

def check_and_update_daily_sl():
    global daily_sl_count, IS_BOT_PAUSED
    try:
        conn = sqlite3.connect(DB_FILE)
        daily_sl_count = conn.cursor().execute("SELECT COUNT(*) FROM trades WHERE created_at >= date('now', 'start of day') AND status LIKE '%SL%'").fetchone()[0]
        conn.close()
        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True; cancel_all_pending_orders_on_pause()
            send_telegram(f"🛑 *[DAILY LOSS LIMIT]*\nKena `{daily_sl_count}x SL`. Bot PAUSE! Aktif besok 07:00 WIB.")
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
                if (side == 'BUY' and curr_price <= en) or (side == 'SELL' and curr_price >= en):
                    n_stat = 'OPEN'; send_telegram(f"🔔 *[ORDER FILLED{tag}]* `{sym}` di {ex_id.upper()}!")
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
                    send_telegram(f"⚪ *[AUTO-CANCEL{tag}]* `{sym}`\nAlasan: `{n_stat}`")
                    c.execute("UPDATE trades SET status=?, bars_count=? WHERE id=?", (n_stat, n_bars, t_id))
                else: c.execute("UPDATE trades SET bars_count=? WHERE id=?", (n_bars, t_id))

            elif status == 'OPEN':
                if (side == 'BUY' and curr_price >= tp) or (side == 'SELL' and curr_price <= tp):
                    c.execute("UPDATE trades SET status='CLOSED (TP)' WHERE id=?", (t_id,))
                    send_telegram(f"🎉 *[TAKE PROFIT{tag}]* `{sym}` | Profit: `+1R` 🚀")
                elif (side == 'BUY' and curr_price <= sl) or (side == 'SELL' and curr_price >= sl):
                    c.execute("UPDATE trades SET status='CLOSED (SL)' WHERE id=?", (t_id,))
                    send_telegram(f"❌ *[STOP LOSS{tag}]* `{sym}` | Loss: `-1R` 🔻")
        except: pass
    conn.commit(); conn.close()

# =================================================================
# MARKET SCANNER
# =================================================================
def scan_markets():
    conn = sqlite3.connect(DB_FILE)
    act_count = conn.cursor().execute("SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')").fetchone()[0]
    conn.close()

    for ex_id, ex in exchanges.items():
        try:
            for sym in [s for s in ex.fetch_tickers().keys() if s.endswith('/USDT:USDT') or s.endswith('/USDT')][:60]:
                if is_manual_alert_symbol(sym):
                    m15, h1, d1 = ex.fetch_ohlcv(sym,'15m',limit=50), ex.fetch_ohlcv(sym,'1h',limit=60), ex.fetch_ohlcv(sym,'1d',limit=2)
                    if m15 and h1 and d1:
                        sig, en, sl, tp = analyze_smc_retrace(pd.DataFrame(m15, columns=['time','open','high','low','close','vol']), pd.DataFrame(h1, columns=['time','open','high','low','close','vol']), d1[-1][1])
                        if sig: send_telegram(f"📢 *[FX ALERT]* `{sym}` | `{sig}`\nEntry: `{en:.5f}` | TP: `{tp:.5f}`")
                    continue

                if act_count >= CONFIG['MAX_CONCURRENT_POSITIONS']: continue

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

                    send_telegram(f"🎯 *{mode}*\nCEX: `{ex_id.upper()}` | `{sym}` | `{sig}`\nEntry: `{en:.5f}`\nMargin: `${CONFIG['MARGIN_PER_TRADE']}` ({lev}x)")
                    act_count += 1
        except Exception as e: send_error_alert(f"Scan Ex [{ex_id}]: {e}")

# =================================================================
# TELEGRAM POLLING
# =================================================================
def telegram_polling():
    last_id = 0
    while True:
        try:
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_id+1}&timeout=10", timeout=15).json()
            if res.get("ok"):
                for r in res.get("result", []):
                    last_id, msg = r["update_id"], r.get("message", {})
                    if str(msg.get("chat", {}).get("id", "")) != str(TELEGRAM_CHAT_ID): continue
                    
                    text, parts = msg.get("text", "").strip(), msg.get("text", "").strip().split(" ")

                    if text.startswith("/setapi ") and len(parts) == 4:
                        ex_id, api, sec = parts[1].lower(), parts[2], parts[3]
                        if ex_id in DEFAULT_CEX:
                            conn = sqlite3.connect(DB_FILE)
                            conn.cursor().execute("UPDATE cex_settings SET api_key=?, secret=? WHERE ex_id=?", (api, sec, ex_id))
                            conn.commit(); conn.close(); reload_exchanges()
                            send_telegram(f"✅ API Key {ex_id.upper()} disimpan!\n\n⚠️ *PENTING:* Hapus pesan ini dari chat demi keamanan!")
                        else: send_telegram("❌ Bursa tidak valid! (bingx/bybit/bitget/binance)")
                    
                    elif text.startswith("/cex ") and len(parts) == 3:
                        action, ex_id = parts[1].lower(), parts[2].lower()
                        if ex_id in DEFAULT_CEX and action in ['on', 'off']:
                            conn = sqlite3.connect(DB_FILE)
                            conn.cursor().execute("UPDATE cex_settings SET enabled=? WHERE ex_id=?", (1 if action=='on' else 0, ex_id))
                            conn.commit(); conn.close(); reload_exchanges()
                            send_telegram(f"✅ CEX {ex_id.upper()} di-{action.upper()}!")

                    elif text == "/dryrun on": CONFIG['DRY_RUN']=True; update_db_setting('dry_run',1.0); send_telegram("✅ *DRY RUN AKTIF*")
                    elif text == "/dryrun off": CONFIG['DRY_RUN']=False; update_db_setting('dry_run',0.0); send_telegram("⚠️ *DRY RUN NONAKTIF* (Uang Asli!)")
                    elif text.startswith("/setmargin ") and len(parts)==2: CONFIG['MARGIN_PER_TRADE']=float(parts[1]); update_db_setting('margin',float(parts[1])); send_telegram(f"✅ Margin: `${parts[1]}`")
                    elif text.startswith("/setmaxpos ") and len(parts)==2: CONFIG['MAX_CONCURRENT_POSITIONS']=int(parts[1]); update_db_setting('max_pos',int(parts[1])); send_telegram(f"✅ Max Posisi: `{parts[1]}`")
                    elif text.startswith("/setlev "):
                        if len(parts) == 2:
                            for ex in DEFAULT_CEX.keys(): CONFIG['LEVERAGE'][ex]=int(parts[1]); update_db_setting(f'lev_{ex}',int(parts[1]))
                            send_telegram(f"✅ Semua leverage: `{parts[1]}x`")
                        elif len(parts) == 3 and parts[1].lower() in DEFAULT_CEX:
                            CONFIG['LEVERAGE'][parts[1].lower()]=int(parts[2]); update_db_setting(f'lev_{parts[1].lower()}',int(parts[2]))
                            send_telegram(f"✅ Leverage {parts[1].upper()}: `{parts[2]}x`")
                    elif text in ["/start", "/help"]:
                        send_telegram("🤖 *[COMMANDS]*\n• `/setapi <cex> <api> <secret>`\n• `/cex on` | `/cex off`\n• `/dryrun on` | `/dryrun off`\n• `/posisi`\n• `/setmargin <angka>`\n• `/setlev <cex> <angka>`\n• `/setmaxpos <angka>`")
                    elif text == "/posisi":
                        conn = sqlite3.connect(DB_FILE); c = conn.cursor()
                        trades = c.execute("SELECT exchange_id, symbol, side, status, order_id FROM trades WHERE status IN ('PENDING', 'OPEN')").fetchall()
                        cex_db = c.execute("SELECT ex_id, api_key, enabled FROM cex_settings").fetchall()
                        conn.close()
                        
                        cex_stat = ""
                        for ex, api, en in cex_db:
                            sts = "🟢 ON" if en else "🔴 OFF"
                            key_sts = "✅ SET" if api else "❌ KOSONG"
                            cex_stat += f"• {ex.upper()} [{sts}] | API: {key_sts}\n"

                        bal = "\n".join([f"• `{k.upper()}` Free: `${get_free_usdt_balance(v):.2f}`" for k,v in exchanges.items()])
                        lev = "\n".join([f"• {k.upper()}: `{v}x`" for k,v in CONFIG['LEVERAGE'].items() if any(e[0]==k and e[2]==1 for e in cex_db)])
                        mode = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
                        
                        m = (f"📊 *[STATUS & POSISI]*\nBot: `{'PAUSED 🛑' if IS_BOT_PAUSED else 'RUNNING 🟢'}`\nMode: `{mode}`\nSL Harian: `{daily_sl_count}/{MAX_DAILY_SL}`\n\n"
                             f"🔌 *Status CEX:*\n{cex_stat}\n"
                             f"⚙️ *Setting:*\n• Margin: `${CONFIG['MARGIN_PER_TRADE']}`\n• Max Pos: `{CONFIG['MAX_CONCURRENT_POSITIONS']}`\n*Leverage:*\n{lev}\n\n"
                             f"💰 *Saldo:*\n{bal}\n\n📋 *Antrean:* {len(trades)}\n")
                        for t in trades: m += f"• `[{t[0].upper()}]` `{t[1]}` {'[SIM]' if str(t[4]).startswith('sim_') else ''} | `{t[2]}` | `{t[3]}`\n"
                        send_telegram(m)
                    elif text == "/pause": IS_BOT_PAUSED = True; send_telegram("⏸️ Bot PAUSED")
                    elif text == "/resume": IS_BOT_PAUSED = False; send_telegram("▶️ Bot RESUMED")
                    elif text == "/cancelall": cancel_all_pending_orders_on_pause()
                    elif text == "/stat":
                        conn = sqlite3.connect(DB_FILE)
                        stats = "\n".join([f"• `{s[0]}`: {s[1]}" for s in conn.cursor().execute("SELECT status, COUNT(*) FROM trades GROUP BY status").fetchall()])
                        conn.close()
                        send_telegram(f"📈 *[STATISTIK]*\n\n{stats}")
        except: time.sleep(5)
        time.sleep(1)

# =================================================================
# MAIN
# =================================================================
app = Flask(__name__)
@app.route('/')
def home(): return "Alchemist Pro v2 Running!"

def main_loop():
    last_hb = time.time()
    send_telegram("🚀 *[ALCHEMIST v2 INITIALIZED]*")
    while True:
        try:
            now = datetime.now(WIB)
            if now.hour == 7 and now.minute == 0 and now.second < 15:
                global daily_sl_count, IS_BOT_PAUSED
                daily_sl_count = 0
                if IS_BOT_PAUSED: IS_BOT_PAUSED = False; send_telegram("🌅 *[NEW DAY RESET]*")
            if time.time() - last_hb >= 21600: send_telegram("💓 *[PING]* Sistem normal."); last_hb = time.time()
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
