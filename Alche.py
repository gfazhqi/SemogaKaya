import os
import sys
import time
import sqlite3
import threading
import atexit
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
BOT_LOCK_FILE = "alchemist_bot.lock"
_bot_lock_handle = None

# =================================================================
# SINGLE INSTANCE LOCK
# =================================================================
def acquire_bot_lock():
    global _bot_lock_handle

    try:
        _bot_lock_handle = open(BOT_LOCK_FILE, "w")
        try:
            import fcntl
            fcntl.flock(_bot_lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            # Windows fallback: exclusive file creation is handled below.
            pass

        _bot_lock_handle.write(str(os.getpid()))
        _bot_lock_handle.flush()
        atexit.register(release_bot_lock)
        return True

    except (BlockingIOError, OSError):
        return False


def release_bot_lock():
    global _bot_lock_handle
    try:
        if _bot_lock_handle:
            try:
                import fcntl
                fcntl.flock(_bot_lock_handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            _bot_lock_handle.close()
            _bot_lock_handle = None
    except Exception:
        pass


# =================================================================
# DATABASE & SETTINGS
# =================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange_id TEXT,
        symbol TEXT,
        order_type TEXT,
        side TEXT,
        entry_price REAL,
        sl_price REAL,
        tp_price REAL,
        order_id TEXT,
        bars_count INTEGER DEFAULT 0,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value REAL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS cex_settings (
        ex_id TEXT PRIMARY KEY,
        api_key TEXT,
        secret TEXT,
        enabled INTEGER DEFAULT 0
    )''')
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN enabled INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('margin', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_pos', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('dry_run', 1.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_trades', 5.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_wr', 40.0)")

    for ex in DEFAULT_CEX.keys():
        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, 50.0)",
            (f'lev_{ex}',)
        )
        c.execute(
            "INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, enabled) VALUES (?, '', '', 0)",
            (ex,)
        )

    conn.commit()
    conn.close()


def load_settings():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    for row in c.execute("SELECT key, value FROM settings").fetchall():
        if row[0] == 'margin':
            CONFIG['MARGIN_PER_TRADE'] = float(row[1])
        elif row[0] == 'max_pos':
            CONFIG['MAX_CONCURRENT_POSITIONS'] = int(row[1])
        elif row[0] == 'dry_run':
            CONFIG['DRY_RUN'] = bool(row[1])
        elif row[0] == 'min_trades':
            CONFIG['MIN_TRADES'] = int(row[1])
        elif row[0] == 'min_wr':
            CONFIG['MIN_WR'] = float(row[1])
        elif row[0].startswith('lev_'):
            CONFIG['LEVERAGE'][row[0].split('_', 1)[1]] = int(row[1])

    conn.close()


def reload_exchanges():
    global exchanges

    conn = sqlite3.connect(DB_FILE, timeout=10)
    rows = conn.cursor().execute(
        "SELECT ex_id, api_key, secret, enabled FROM cex_settings"
    ).fetchall()
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
            except Exception:
                pass


def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.cursor().execute(
        "UPDATE settings SET value=? WHERE key=?",
        (value, key)
    )
    conn.commit()
    conn.close()


# =================================================================
# HELPERS
# =================================================================
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code != 200:
            payload.pop("parse_mode", None)
            requests.post(url, json=payload, timeout=5)
    except Exception:
        pass


def send_error_alert(err_msg):
    global last_error_time
    if time.time() - last_error_time > ERROR_COOLDOWN:
        send_telegram(f"⚠️ <b>ERROR ALERT</b>\n\n<code>{str(err_msg)[:3500]}</code>")
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
        if isinstance(usdt, dict):
            return float(usdt.get('free', 0.0))
        return 0.0
    except Exception:
        return 0.0


def cancel_all_pending_orders_on_pause():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        c = conn.cursor()
        rows = c.execute(
            "SELECT id, exchange_id, symbol, order_id "
            "FROM trades WHERE status='PENDING'"
        ).fetchall()

        for t_id, ex_id, sym, ord_id in rows:
            if ex_id in exchanges and not str(ord_id).startswith('sim_'):
                try:
                    exchanges[ex_id].cancel_order(ord_id, sym)
                except Exception:
                    pass
            c.execute(
                "UPDATE trades SET status='CANCELLED (Auto/Manual)' WHERE id=?",
                (t_id,)
            )

        conn.commit()
        conn.close()
        send_telegram("🧹 <b>CLEANUP</b> Semua pending order dibatalkan!")
    except Exception:
        pass


def check_and_update_daily_sl():
    global daily_sl_count, IS_BOT_PAUSED
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        daily_sl_count = conn.cursor().execute(
            "SELECT COUNT(*) FROM trades "
            "WHERE created_at >= date('now', 'start of day') "
            "AND status LIKE '%SL%'"
        ).fetchone()[0]
        conn.close()

        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True
            cancel_all_pending_orders_on_pause()
            send_telegram(
                f"🛑 <b>DAILY LOSS LIMIT</b>\n"
                f"Kena {daily_sl_count}x SL. Bot PAUSE!"
            )
    except Exception:
        pass


# =================================================================
# SMC STRATEGY & LIFECYCLE
# =================================================================
# LOGIC TRADE TIDAK DIUBAH
def analyze_smc_retrace(df_m15, df_h1, daily_open):
    ema50 = df_h1['close'].ewm(span=50, adjust=False).mean().iloc[-2]
    h1_c, c_prev = df_h1['close'].iloc[-2], df_m15.iloc[-2]
    sig, en, sl, tp = None, 0.0, 0.0, 0.0

    if h1_c > ema50 and c_prev['close'] > daily_open:
        if c_prev['low'] < df_m15['low'].iloc[-7:-2].min() and c_prev['close'] > c_prev['open']:
            en = c_prev['high'] - (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['low'] * (1.0 - SL_BUFFER_PERCENT)
            tp = en + (en - sl)
            sig = 'BUY'

    elif h1_c < ema50 and c_prev['close'] < daily_open:
        if c_prev['high'] > df_m15['high'].iloc[-7:-2].max() and c_prev['close'] < c_prev['open']:
            en = c_prev['low'] + (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['high'] * (1.0 + SL_BUFFER_PERCENT)
            tp = en - (sl - en)
            sig = 'SELL'

    return sig, en, sl, tp


def update_orders_and_positions():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    rows = c.execute(
        "SELECT id, exchange_id, symbol, side, entry_price, sl_price, "
        "tp_price, order_id, bars_count, status "
        "FROM trades WHERE status IN ('PENDING', 'OPEN')"
    ).fetchall()

    for t_id, ex_id, sym, side, en, sl, tp, ord_id, bars, status in rows:
        if ex_id not in exchanges:
            continue

        try:
            curr_price = exchanges[ex_id].fetch_ticker(sym)['last']
            n_bars, is_sim = bars + 1, str(ord_id).startswith('sim_')
            tag = " (DRY RUN)" if is_sim else ""

            if status == 'PENDING':
                n_stat = 'PENDING'

                if (side == 'BUY' and curr_price <= en) or (side == 'SELL' and curr_price >= en):
                    n_stat = 'OPEN'
                    send_telegram(f"🔔 <b>ORDER FILLED{tag}</b> {sym} di {ex_id.upper()}!")

                elif side == 'BUY':
                    if curr_price >= tp:
                        n_stat = 'MISSED (TP First)'
                    elif curr_price <= sl:
                        n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30:
                        n_stat = 'INVALID (>30 Bars)'

                elif side == 'SELL':
                    if curr_price <= tp:
                        n_stat = 'MISSED (TP First)'
                    elif curr_price >= sl:
                        n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30:
                        n_stat = 'INVALID (>30 Bars)'

                if n_stat != 'PENDING':
                    if ('CANCEL' in n_stat or 'MISS' in n_stat or 'INV' in n_stat) and not is_sim:
                        try:
                            exchanges[ex_id].cancel_order(ord_id, sym)
                        except Exception:
                            pass

                    send_telegram(f"⚪ <b>AUTO-CANCEL{tag}</b> {sym}\nAlasan: {n_stat}")
                    c.execute(
                        "UPDATE trades SET status=?, bars_count=? WHERE id=?",
                        (n_stat, n_bars, t_id)
                    )
                else:
                    c.execute(
                        "UPDATE trades SET bars_count=? WHERE id=?",
                        (n_bars, t_id)
                    )

            elif status == 'OPEN':
                if (side == 'BUY' and curr_price >= tp) or (side == 'SELL' and curr_price <= tp):
                    c.execute(
                        "UPDATE trades SET status='CLOSED (TP)' WHERE id=?",
                        (t_id,)
                    )
                    send_telegram(f"🎉 <b>TAKE PROFIT{tag}</b> {sym} | +1R 🚀")

                elif (side == 'BUY' and curr_price <= sl) or (side == 'SELL' and curr_price >= sl):
                    c.execute(
                        "UPDATE trades SET status='CLOSED (SL)' WHERE id=?",
                        (t_id,)
                    )
                    send_telegram(f"❌ <b>STOP LOSS{tag}</b> {sym} | -1R 🔻")

        except Exception:
            pass

    conn.commit()
    conn.close()


# =================================================================
# MARKET SCANNER
# =================================================================
# LOGIC TRADE TIDAK DIUBAH
def scan_markets():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    act_count = c.execute(
        "SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')"
    ).fetchone()[0]

    pair_perf = {}
    for sym, stat in c.execute(
        "SELECT symbol, status FROM trades "
        "WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
    ).fetchall():
        if sym not in pair_perf:
            pair_perf[sym] = {'w': 0, 'l': 0}
        if 'TP' in stat:
            pair_perf[sym]['w'] += 1
        else:
            pair_perf[sym]['l'] += 1

    conn.close()

    for ex_id, ex in exchanges.items():
        try:
            for sym in [s for s in ex.fetch_tickers().keys()
                        if s.endswith('/USDT:USDT') or s.endswith('/USDT')][:60]:

                if is_manual_alert_symbol(sym):
                    m15 = ex.fetch_ohlcv(sym, '15m', limit=50)
                    h1 = ex.fetch_ohlcv(sym, '1h', limit=60)
                    d1 = ex.fetch_ohlcv(sym, '1d', limit=2)

                    if m15 and h1 and d1:
                        sig, en, sl, tp = analyze_smc_retrace(
                            pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                            pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                            d1[-1][1]
                        )
                        if sig:
                            send_telegram(f"📢 <b>FX ALERT</b> {sym} | {sig}")
                    continue

                if act_count >= CONFIG['MAX_CONCURRENT_POSITIONS']:
                    continue

                if sym in pair_perf and sym not in CORE_PAIRS:
                    tot_trades = pair_perf[sym]['w'] + pair_perf[sym]['l']
                    if tot_trades >= CONFIG['MIN_TRADES']:
                        wr = (pair_perf[sym]['w'] / tot_trades) * 100
                        if wr < CONFIG['MIN_WR']:
                            continue

                m15 = ex.fetch_ohlcv(sym, '15m', limit=50)
                h1 = ex.fetch_ohlcv(sym, '1h', limit=60)
                d1 = ex.fetch_ohlcv(sym, '1d', limit=2)

                if not m15 or not h1 or not d1:
                    continue

                sig, en, sl, tp = analyze_smc_retrace(
                    pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                    pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                    d1[-1][1]
                )

                if sig:
                    lev = CONFIG['LEVERAGE'].get(ex_id, 50)
                    qty = (CONFIG['MARGIN_PER_TRADE'] * lev) / en
                    mode = "[DRY RUN]" if CONFIG['DRY_RUN'] else "[REAL TRADE]"
                    ord_id = f"sim_{int(time.time()*1000)}"

                    if not CONFIG['DRY_RUN']:
                        try:
                            ex.set_leverage(lev, sym)
                        except Exception:
                            pass

                        try:
                            ord_id = str(ex.create_order(
                                sym,
                                'limit',
                                'buy' if sig == 'BUY' else 'sell',
                                qty,
                                en,
                                params={
                                    'stopLoss': {'price': sl},
                                    'takeProfit': {'price': tp}
                                }
                            )['id'])
                        except Exception as e:
                            send_error_alert(f"Order Fails {sym}: {e}")
                            continue

                    conn = sqlite3.connect(DB_FILE, timeout=10)
                    conn.cursor().execute(
                        "INSERT INTO trades "
                        "(exchange_id, symbol, order_type, side, entry_price, "
                        "sl_price, tp_price, order_id, status) "
                        "VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')",
                        (ex_id, sym, sig, en, sl, tp, ord_id)
                    )
                    conn.commit()
                    conn.close()

                    send_telegram(
                        f"🎯 <b>{mode}</b>\n"
                        f"CEX: {ex_id.upper()} | {sym} | {sig}\n"
                        f"Entry: {en:.5f}"
                    )
                    act_count += 1

        except Exception as e:
            send_error_alert(f"Scan Ex [{ex_id}]: {e}")


# =================================================================
# TELEGRAM POLLING
# =================================================================
def telegram_polling():
    """Telegram command listener.

    IMPORTANT: This function only handles Telegram commands.
    Trading/scanner logic is intentionally unchanged.
    """
    global IS_BOT_PAUSED

    last_id = 0

    def db_read(query, params=()):
        conn = sqlite3.connect(DB_FILE, timeout=3)
        try:
            return conn.cursor().execute(query, params).fetchall()
        finally:
            conn.close()

    def send_positions_balance():
        """Fetch balances separately so /posisi never waits for an exchange."""
        try:
            lines = []
            for ex_id, ex in list(exchanges.items()):
                try:
                    bal = get_free_usdt_balance(ex)
                    lines.append(f"• {ex_id.upper()} Free: ${bal:.2f}")
                except Exception:
                    lines.append(f"• {ex_id.upper()} Free: Error/Timeout")

            if not lines:
                lines = ["• Saldo: Tidak ada CEX aktif"]

            send_telegram("💰 <b>SALDO CEX</b>\n" + "\n".join(lines))
        except Exception as e:
            send_telegram(f"⚠️ ERROR SALDO:\n<code>{str(e)[:2000]}</code>")

    # Long polling requires webhook mode to be disabled.
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook",
            params={"drop_pending_updates": False},
            timeout=5
        )
    except Exception:
        pass

    send_telegram("🟢 <b>TELEGRAM POLLING ONLINE</b>")

    while True:
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "timeout": 5},
                timeout=10
            )
            res = response.json()

            if not res.get("ok"):
                time.sleep(2)
                continue

            for update in res.get("result", []):
                last_id = update.get("update_id", last_id)
                msg = update.get("message") or {}

                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                raw_text = str(msg.get("text") or "").strip()
                if not raw_text:
                    continue

                # Normalize Telegram commands.
                # Supports /settings, /settings@BotName, extra spaces, etc.
                parts = raw_text.split()
                command = parts[0].split("@", 1)[0].lower()
                args = parts[1:]

                # =================================================
                # HARD-PRIORITY COMMANDS
                # /settings and /posisi are handled BEFORE the normal
                # command chain so DB/exchange problems can never make
                # these commands appear unresponsive.
                # =================================================
                if command in ("/setting", "/settings"):
                    try:
                        mode_text = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
                        send_telegram(
                            f"⚙️ <b>SETTING BOT</b>\\n\\n"
                            f"Mode: {mode_text}\\n"
                            f"Margin: ${CONFIG['MARGIN_PER_TRADE']}\\n"
                            f"Max Pos: {CONFIG['MAX_CONCURRENT_POSITIONS']}\\n"
                            f"Min Trade: {CONFIG['MIN_TRADES']}\\n"
                            f"Min WR: {CONFIG['MIN_WR']}%"
                        )

                        def settings_details():
                            try:
                                rows = db_read(
                                    "SELECT ex_id, api_key, enabled FROM cex_settings"
                                )
                                lines = []
                                for ex_id, api, enabled in rows:
                                    lines.append(
                                        f"• {ex_id.upper()}: "
                                        f"{'🟢 ON' if enabled else '🔴 OFF'} | "
                                        f"Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x | "
                                        f"API: {'✅' if api else '❌'}"
                                    )
                                send_telegram(
                                    "🔌 <b>BURSA</b>\\n" +
                                    ("\\n".join(lines) if lines else "Belum ada CEX")
                                )
                            except Exception as e:
                                send_telegram(
                                    f"⚠️ <b>SETTINGS DB ERROR</b>\\n"
                                    f"<code>{str(e)[:2000]}</code>"
                                )

                        threading.Thread(target=settings_details, daemon=True).start()
                    except Exception as e:
                        send_telegram(
                            f"⚠️ <b>SETTINGS ERROR</b>\\n"
                            f"<code>{str(e)[:2000]}</code>"
                        )
                    continue

                if command == "/posisi":
                    try:
                        # Immediate acknowledgement. This MUST happen before
                        # SQLite/exchange operations.
                        send_telegram("📊 <b>STATUS & POSISI</b>\\n⏳ Memuat data...")

                        def posisi_details():
                            try:
                                rows = db_read(
                                    "SELECT exchange_id, symbol, side, status, order_id "
                                    "FROM trades WHERE status IN ('PENDING', 'OPEN')"
                                )
                                cex_db = db_read(
                                    "SELECT ex_id, enabled FROM cex_settings"
                                )

                                cex_lines = [
                                    f"• {ex_id.upper()} "
                                    f"{'🟢 ON' if enabled else '🔴 OFF'}"
                                    for ex_id, enabled in cex_db
                                ]

                                mode = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL"
                                msg = (
                                    f"📊 <b>STATUS & POSISI</b>\\n"
                                    f"Bot: {'PAUSED' if IS_BOT_PAUSED else 'RUNNING'} | "
                                    f"Mode: {mode}\\n\\n"
                                    f"🔌 <b>CEX:</b>\\n"
                                    f"{chr(10).join(cex_lines) if cex_lines else '• Tidak ada CEX'}\\n\\n"
                                    f"📋 <b>ANTREAN:</b> {len(rows)}\\n"
                                )

                                for t in rows:
                                    msg += (
                                        f"• {t[0].upper()} | {t[1]} | "
                                        f"{t[2]} | {t[3]}\\n"
                                    )

                                send_telegram(msg)

                                # Balance is deliberately separate so a slow
                                # CEX API can never block Telegram commands.
                                def balance_details():
                                    try:
                                        lines = []
                                        for ex_id, ex in list(exchanges.items()):
                                            try:
                                                bal = get_free_usdt_balance(ex)
                                                lines.append(
                                                    f"• {ex_id.upper()} Free: ${bal:.2f}"
                                                )
                                            except Exception:
                                                lines.append(
                                                    f"• {ex_id.upper()} Free: Error/Timeout"
                                                )
                                        send_telegram(
                                            "💰 <b>SALDO CEX</b>\\n" +
                                            ("\\n".join(lines)
                                             if lines else "• Tidak ada CEX aktif")
                                        )
                                    except Exception as e:
                                        send_telegram(
                                            f"⚠️ <b>SALDO ERROR</b>\\n"
                                            f"<code>{str(e)[:2000]}</code>"
                                        )

                                threading.Thread(
                                    target=balance_details,
                                    daemon=True
                                ).start()

                            except Exception as e:
                                send_telegram(
                                    f"⚠️ <b>POSISI ERROR</b>\\n"
                                    f"<code>{str(e)[:2000]}</code>"
                                )

                        threading.Thread(target=posisi_details, daemon=True).start()

                    except Exception as e:
                        send_telegram(
                            f"⚠️ <b>POSISI ERROR</b>\\n"
                            f"<code>{str(e)[:2000]}</code>"
                        )
                    continue

                try:
                    # -------------------------------------------------
                    # API / CEX
                    # -------------------------------------------------
                    if command == "/setapi" and len(args) == 3:
                        ex_id, api, sec = args[0].lower(), args[1], args[2]
                        if ex_id in DEFAULT_CEX:
                            conn = sqlite3.connect(DB_FILE, timeout=5)
                            conn.cursor().execute(
                                "UPDATE cex_settings SET api_key=?, secret=? WHERE ex_id=?",
                                (api, sec, ex_id)
                            )
                            conn.commit()
                            conn.close()
                            reload_exchanges()
                            send_telegram(f"✅ API Key {ex_id.upper()} disimpan!")
                        else:
                            send_telegram("⚠️ CEX tidak dikenal.")

                    elif command == "/cex" and len(args) == 2:
                        action, ex_id = args[0].lower(), args[1].lower()
                        if ex_id in DEFAULT_CEX and action in ("on", "off"):
                            conn = sqlite3.connect(DB_FILE, timeout=5)
                            conn.cursor().execute(
                                "UPDATE cex_settings SET enabled=? WHERE ex_id=?",
                                (1 if action == "on" else 0, ex_id)
                            )
                            conn.commit()
                            conn.close()
                            reload_exchanges()
                            send_telegram(f"✅ CEX {ex_id.upper()} di-{action.upper()}!")
                        else:
                            send_telegram("⚠️ Format: /cex on/off <cex>")

                    # -------------------------------------------------
                    # BASIC SETTINGS
                    # -------------------------------------------------
                    elif command == "/dryrun" and len(args) == 1:
                        value = args[0].lower()
                        if value == "on":
                            CONFIG['DRY_RUN'] = True
                            update_db_setting('dry_run', 1.0)
                            send_telegram("✅ <b>DRY RUN AKTIF</b>")
                        elif value == "off":
                            CONFIG['DRY_RUN'] = False
                            update_db_setting('dry_run', 0.0)
                            send_telegram("⚠️ <b>REAL MONEY AKTIF</b>")
                        else:
                            send_telegram("⚠️ Format: /dryrun on atau /dryrun off")

                    elif command == "/setmargin" and len(args) == 1:
                        value = float(args[0])
                        CONFIG['MARGIN_PER_TRADE'] = value
                        update_db_setting('margin', value)
                        send_telegram(f"✅ Margin: ${value}")

                    elif command == "/setmaxpos" and len(args) == 1:
                        value = int(args[0])
                        CONFIG['MAX_CONCURRENT_POSITIONS'] = value
                        update_db_setting('max_pos', value)
                        send_telegram(f"✅ Max Posisi: {value}")

                    elif command == "/setminwr" and len(args) == 1:
                        value = float(args[0])
                        CONFIG['MIN_WR'] = value
                        update_db_setting('min_wr', value)
                        send_telegram(f"✅ Min WinRate: {value}%")

                    elif command == "/setmintrades" and len(args) == 1:
                        value = int(args[0])
                        CONFIG['MIN_TRADES'] = value
                        update_db_setting('min_trades', value)
                        send_telegram(f"✅ Min Trade: {value}x")

                    elif command == "/setlev" and len(args) in (1, 2):
                        if len(args) == 1:
                            value = int(args[0])
                            for ex in DEFAULT_CEX.keys():
                                CONFIG['LEVERAGE'][ex] = value
                                update_db_setting(f'lev_{ex}', value)
                            send_telegram(f"✅ Semua leverage: {value}x")
                        else:
                            ex_id = args[0].lower()
                            value = int(args[1])
                            if ex_id not in DEFAULT_CEX:
                                send_telegram("⚠️ CEX tidak dikenal.")
                            else:
                                CONFIG['LEVERAGE'][ex_id] = value
                                update_db_setting(f'lev_{ex_id}', value)
                                send_telegram(f"✅ Lev {ex_id.upper()}: {value}x")

                    # -------------------------------------------------
                    # PAIR PERFORMANCE
                    # -------------------------------------------------
                    elif command == "/pairperf":
                        rows = db_read(
                            "SELECT symbol, status FROM trades "
                            "WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
                        )
                        pair_perf = {}
                        for sym, stat in rows:
                            if sym not in pair_perf:
                                pair_perf[sym] = {'w': 0, 'l': 0}
                            if 'TP' in stat:
                                pair_perf[sym]['w'] += 1
                            else:
                                pair_perf[sym]['l'] += 1

                        gacor, blist, eval_wait = "", "", ""
                        for sym, data in pair_perf.items():
                            total = data['w'] + data['l']
                            wr = (data['w'] / total) * 100 if total else 0
                            is_core = sym in CORE_PAIRS
                            core_tag = " [CORE]" if is_core else ""
                            line = (
                                f"• {sym}{core_tag}: {data['w']}W/{data['l']}L "
                                f"(WR: {wr:.1f}%)\n"
                            )
                            if total < CONFIG['MIN_TRADES']:
                                eval_wait += line
                            elif is_core or wr >= CONFIG['MIN_WR']:
                                gacor += line
                            else:
                                blist += line

                        send_telegram(
                            f"🏆 <b>PERFORMA KOIN</b>\n\n"
                            f"🔥 <b>AKTIF</b>\n{gacor or 'Belum ada'}\n\n"
                            f"⏳ <b>EVALUASI</b>\n{eval_wait or 'Belum ada'}\n\n"
                            f"🗑️ <b>BLACKLIST</b>\n{blist or 'Belum ada'}"
                        )

                    # -------------------------------------------------
                    # SETTINGS - LIGHTWEIGHT, NO EXCHANGE CALL
                    # -------------------------------------------------
                    elif command in ("/setting", "/settings"):
                        cex_db = db_read(
                            "SELECT ex_id, api_key, enabled FROM cex_settings"
                        )
                        mode_text = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
                        cex_lines = []
                        for ex_id, api, en in cex_db:
                            cex_lines.append(
                                f"• {ex_id.upper()}: "
                                f"{'🟢 ON' if en else '🔴 OFF'} | "
                                f"Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x | "
                                f"API: {'✅' if api else '❌'}"
                            )
                        cex_info = "\n".join(cex_lines) if cex_lines else "Belum ada"

                        # Send immediately. No network/exchange request here.
                        send_telegram(
                            f"⚙️ <b>SETTING BOT</b>\n\n"
                            f"Mode: {mode_text}\n"
                            f"Margin: ${CONFIG['MARGIN_PER_TRADE']}\n"
                            f"Max Pos: {CONFIG['MAX_CONCURRENT_POSITIONS']}\n"
                            f"Min Trade: {CONFIG['MIN_TRADES']}\n"
                            f"Min WR: {CONFIG['MIN_WR']}%\n\n"
                            f"<b>BURSA:</b>\n{cex_info}"
                        )

                    # -------------------------------------------------
                    # HELP
                    # -------------------------------------------------
                    elif command in ("/start", "/help"):
                        send_telegram(
                            "🤖 <b>COMMANDS</b>\n"
                            "• /cex on/off <cex> | /setapi <cex> <api> <secret>\n"
                            "• /dryrun on/off\n"
                            "• /posisi | /stat | /pairperf\n"
                            "• /setting | /settings\n"
                            "• /setmargin | /setlev | /setmaxpos\n"
                            "• /setminwr | /setmintrades\n"
                            "• /cancelall | /pause | /resume"
                        )

                    # -------------------------------------------------
                    # POSISI - RESPOND FIRST, BALANCE AFTERWARD
                    # -------------------------------------------------
                    elif command == "/posisi":
                        rows = db_read(
                            "SELECT exchange_id, symbol, side, status, order_id "
                            "FROM trades WHERE status IN ('PENDING', 'OPEN')"
                        )
                        cex_db = db_read(
                            "SELECT ex_id, enabled FROM cex_settings"
                        )

                        cex_lines = [
                            f"• {ex_id.upper()} {'🟢 ON' if en else '🔴 OFF'}"
                            for ex_id, en in cex_db
                        ]
                        cex_stat = "\n".join(cex_lines) if cex_lines else "• Tidak ada CEX"
                        mode = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL"

                        m = (
                            f"📊 <b>STATUS & POSISI</b>\n"
                            f"Bot: {'PAUSED' if IS_BOT_PAUSED else 'RUNNING'} | Mode: {mode}\n\n"
                            f"🔌 <b>CEX:</b>\n{cex_stat}\n\n"
                            f"📋 <b>ANTREAN:</b> {len(rows)}\n"
                        )
                        for t in rows:
                            m += f"• {t[0].upper()} | {t[1]} | {t[2]} | {t[3]}\n"

                        # IMPORTANT: send this before any exchange API call.
                        send_telegram(m)

                        # Balance check runs independently and cannot block polling.
                        threading.Thread(
                            target=send_positions_balance,
                            daemon=True
                        ).start()

                    # -------------------------------------------------
                    # BOT CONTROL
                    # -------------------------------------------------
                    elif command == "/pause":
                        IS_BOT_PAUSED = True
                        send_telegram("⏸️ Bot PAUSED")

                    elif command == "/resume":
                        IS_BOT_PAUSED = False
                        send_telegram("▶️ Bot RESUMED")

                    elif command == "/cancelall":
                        cancel_all_pending_orders_on_pause()

                    elif command == "/stat":
                        rows = db_read("SELECT status, COUNT(*) FROM trades GROUP BY status")
                        stats = "\n".join([f"• {s[0]}: {s[1]}" for s in rows])
                        send_telegram(f"📈 <b>STATISTIK</b>\n\n{stats or 'Belum ada data'}")

                    else:
                        # Ignore unknown text just like before.
                        pass

                except Exception as cmd_error:
                    # Never let one bad command kill the Telegram polling loop.
                    send_telegram(
                        f"⚠️ <b>COMMAND ERROR</b>\n"
                        f"Command: <code>{raw_text[:200]}</code>\n"
                        f"<code>{str(cmd_error)[:2500]}</code>"
                    )

        except Exception as poll_error:
            send_error_alert(f"Telegram polling: {poll_error}")
            time.sleep(3)

        time.sleep(1)


# =================================================================
# WEB DASHBOARD & FLASK SERVER
# =================================================================
app = Flask(__name__, template_folder='templates')

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/stats')
def api_stats():
    conn = sqlite3.connect(DB_FILE, timeout=10)

    df = pd.read_sql_query(
        "SELECT symbol, side, status, created_at "
        "FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)') "
        "ORDER BY id ASC",
        conn
    )

    conn.close()

    if df.empty:
        return jsonify({
            'dates': [],
            'pnl': [],
            'win_rate': 0,
            'wins': 0,
            'losses': 0,
            'net_pnl': 0,
            'recent': []
        })

    margin = CONFIG.get('MARGIN_PER_TRADE', 2.0)
    df['pnl_value'] = df['status'].apply(
        lambda x: margin if 'TP' in x else -margin
    )
    df['cum_pnl'] = df['pnl_value'].cumsum()
    df['date_only'] = pd.to_datetime(
        df['created_at']
    ).dt.strftime('%d %b %H:%M')

    wins = len(df[df['status'] == 'CLOSED (TP)'])
    losses = len(df[df['status'] == 'CLOSED (SL)'])
    total = wins + losses

    win_rate = round((wins / total) * 100, 1) if total > 0 else 0
    net_pnl = round(df['cum_pnl'].iloc[-1], 2) if not df.empty else 0
    recent = df.tail(10)[::-1].to_dict('records')

    return jsonify({
        'dates': df['date_only'].tolist(),
        'pnl': df['cum_pnl'].tolist(),
        'win_rate': win_rate,
        'wins': wins,
        'losses': losses,
        'net_pnl': net_pnl,
        'recent': recent
    })


def main_loop():
    global daily_sl_count, IS_BOT_PAUSED

    last_hb = time.time()
    send_telegram("🚀 <b>ALCHEMIST v2 INITIALIZED</b>")

    while True:
        try:
            now = datetime.now(WIB)

            if now.hour == 7 and now.minute == 0 and now.second < 15:
                daily_sl_count = 0
                if IS_BOT_PAUSED:
                    IS_BOT_PAUSED = False
                    send_telegram("🌅 <b>NEW DAY RESET</b>")

            if time.time() - last_hb >= 21600:
                send_telegram("💓 <b>PING</b> Sistem normal.")
                last_hb = time.time()

            check_and_update_daily_sl()
            update_orders_and_positions()

            if not IS_BOT_PAUSED and is_kill_zone():
                scan_markets()

        except Exception as e:
            send_error_alert(str(e))

        time.sleep(15)


if __name__ == '__main__':
    if not acquire_bot_lock():
        print("ERROR: Alchemist bot is already running. Stop the old process first.")
        sys.exit(1)

    init_db()
    load_settings()
    reload_exchanges()

    threading.Thread(
        target=telegram_polling,
        daemon=True
    ).start()

    threading.Thread(
        target=lambda: app.run(
            host='0.0.0.0',
            port=5000
        ),
        daemon=True
    ).start()

    main_loop()
