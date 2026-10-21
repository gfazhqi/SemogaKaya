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
def telegram_api(method, payload=None, timeout=10):
    """Call Telegram Bot API and ALWAYS return a dict instead of raising."""
    payload = payload or {}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {
                "ok": False,
                "description": f"HTTP {r.status_code}: {r.text[:500]}"
            }

        if not data.get("ok"):
            print(
                f"[TELEGRAM API ERROR] {method}: "
                f"{data.get('description', 'unknown error')}",
                flush=True
            )
        return data
    except requests.RequestException as e:
        print(f"[TELEGRAM NETWORK ERROR] {method}: {e}", flush=True)
        return {"ok": False, "description": str(e)}
    except Exception as e:
        print(f"[TELEGRAM ERROR] {method}: {e}", flush=True)
        return {"ok": False, "description": str(e)}


def send_telegram(message):
    """Send Telegram message. If HTML fails, retry as plain text."""
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': str(message),
        'parse_mode': 'HTML'
    }
    result = telegram_api('sendMessage', payload, timeout=10)
    if result.get('ok'):
        return True

    # HTML can fail because of an unescaped symbol. Retry without parse mode.
    result = telegram_api('sendMessage', {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': str(message)
    }, timeout=10)
    return bool(result.get('ok'))


def validate_exchange_credentials(ex_id):
    """Validate CEX credentials with a harmless authenticated balance request."""
    if ex_id not in DEFAULT_CEX:
        return False, 'CEX tidak dikenal.'

    try:
        conn = sqlite3.connect(DB_FILE, timeout=5)
        row = conn.cursor().execute(
            'SELECT api_key, secret FROM cex_settings WHERE ex_id=?', (ex_id,)
        ).fetchone()
        conn.close()

        if not row or not row[0] or not row[1]:
            return False, 'API Key/Secret belum disimpan.'

        # Build a temporary client so validation never depends on whether
        # the exchange is currently enabled.
        ex = getattr(ccxt, ex_id)({
            'apiKey': row[0],
            'secret': row[1],
            'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
            'enableRateLimit': True,
            'timeout': 8000
        })
        ex.fetch_balance()
        return True, 'API valid dan autentikasi berhasil.'

    except ccxt.AuthenticationError as e:
        return False, f'API ditolak / tidak valid: {e}'
    except ccxt.PermissionDenied as e:
        return False, f'API valid tetapi permission ditolak: {e}'
    except ccxt.NetworkError as e:
        return False, f'Gagal terhubung ke {ex_id.upper()}: {e}'
    except Exception as e:
        return False, f'Validasi gagal: {e}'


def telegram_polling():
    """Reliable Telegram long-polling command processor."""
    global IS_BOT_PAUSED

    offset_file = "telegram_offset.txt"
    try:
        last_id = int(open(offset_file).read().strip())
    except Exception:
        last_id = 0

    telegram_api('deleteWebhook', {'drop_pending_updates': False}, timeout=10)

    me = telegram_api('getMe', {}, timeout=10)
    if not me.get('ok'):
        print("[FATAL] Bot token invalid/unreachable.", flush=True)
        return

    bot_name = me.get('result', {}).get('username', 'unknown')
    print(f"[TELEGRAM] ONLINE @{bot_name} | CHAT_ID={TELEGRAM_CHAT_ID}", flush=True)
    send_telegram("🟢 <b>TELEGRAM ONLINE</b>\nKirim /settings untuk test.")

    while True:
        try:
            data = telegram_api(
                'getUpdates',
                {
                    'offset': last_id + 1,
                    'timeout': 20,
                    'allowed_updates': ['message']
                },
                timeout=30
            )

            if not data.get('ok'):
                desc = data.get('description', 'unknown error')
                print(f"[POLLING] getUpdates error: {desc}", flush=True)
                time.sleep(3)
                continue

            for update in data.get('result', []):
                update_id = int(update.get('update_id', 0))
                if update_id > last_id:
                    last_id = update_id
                    try:
                        with open(offset_file, "w") as f:
                            f.write(str(last_id))
                    except Exception:
                        pass

                msg = update.get('message') or {}
                chat_id = str(msg.get('chat', {}).get('id', ''))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    print(f"[TELEGRAM] ignored chat_id={chat_id}", flush=True)
                    continue

                raw = str(msg.get('text') or '').strip()
                if not raw:
                    continue

                parts = raw.split()
                command = parts[0].split('@', 1)[0].lower()
                args = parts[1:]
                print(f"[COMMAND] {command} {args}", flush=True)

                # ---------------------------------------------------------
                # /settings and /setting: NO CEX CALL, NO NETWORK CALL.
                # This must always respond immediately.
                # ---------------------------------------------------------
                if command in ("/settings", "/setting"):
                    try:
                        mode = "🟢 DRY RUN" if CONFIG["DRY_RUN"] else "⚠️ REAL MONEY"
                        send_telegram(
                            "⚙️ <b>SETTING BOT</b>\n\n"
                            f"Mode: {mode}\n"
                            f"Margin: ${CONFIG['MARGIN_PER_TRADE']}\n"
                            f"Max Pos: {CONFIG['MAX_CONCURRENT_POSITIONS']}\n"
                            f"Min Trade: {CONFIG['MIN_TRADES']}\n"
                            f"Min WR: {CONFIG['MIN_WR']}%"
                        )

                        def settings_worker():
                            try:
                                conn = sqlite3.connect(DB_FILE, timeout=3)
                                rows = conn.execute(
                                    "SELECT ex_id, api_key, enabled FROM cex_settings"
                                ).fetchall()
                                conn.close()

                                lines = []
                                for ex_id, api, enabled in rows:
                                    lines.append(
                                        f"• {ex_id.upper()}: "
                                        f"{'🟢 ON' if enabled else '🔴 OFF'} | "
                                        f"Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x | "
                                        f"API: {'✅' if api else '❌'}"
                                    )

                                send_telegram(
                                    "🔌 <b>BURSA</b>\n" +
                                    ("\n".join(lines) if lines else "Belum ada CEX")
                                )
                            except Exception as e:
                                send_telegram(
                                    f"⚠️ <b>SETTINGS DB ERROR</b>\n<code>{str(e)[:2000]}</code>"
                                )

                        threading.Thread(target=settings_worker, daemon=True).start()
                    except Exception as e:
                        send_telegram(
                            f"⚠️ <b>SETTINGS ERROR</b>\n<code>{str(e)[:2000]}</code>"
                        )
                    continue

                # ---------------------------------------------------------
                # /posisi: immediate response, then DB/balance separately.
                # ---------------------------------------------------------
                if command == "/posisi":
                    try:
                        send_telegram("📊 <b>STATUS & POSISI</b>\n⏳ Mengambil data...")

                        def posisi_worker():
                            try:
                                conn = sqlite3.connect(DB_FILE, timeout=3)
                                trades = conn.execute(
                                    "SELECT exchange_id, symbol, side, status "
                                    "FROM trades WHERE status IN ('PENDING','OPEN')"
                                ).fetchall()
                                cex_rows = conn.execute(
                                    "SELECT ex_id, enabled FROM cex_settings"
                                ).fetchall()
                                conn.close()

                                mode = "🟢 DRY RUN" if CONFIG["DRY_RUN"] else "⚠️ REAL"
                                text = (
                                    "📊 <b>STATUS & POSISI</b>\n"
                                    f"Bot: {'PAUSED' if IS_BOT_PAUSED else 'RUNNING'} | Mode: {mode}\n\n"
                                    "🔌 <b>CEX:</b>\n"
                                )

                                text += "\n".join(
                                    f"• {ex.upper()} {'🟢 ON' if en else '🔴 OFF'}"
                                    for ex, en in cex_rows
                                ) or "• Tidak ada CEX"

                                text += f"\n\n📋 <b>ANTREAN:</b> {len(trades)}\n"
                                for ex, sym, side, status in trades:
                                    text += f"• {ex.upper()} | {sym} | {side} | {status}\n"

                                send_telegram(text)

                                def balance_worker():
                                    lines = []
                                    for ex_id, ex in list(exchanges.items()):
                                        try:
                                            bal = get_free_usdt_balance(ex)
                                            lines.append(f"• {ex_id.upper()} Free: ${bal:.2f}")
                                        except Exception as e:
                                            lines.append(
                                                f"• {ex_id.upper()} Error: {str(e)[:120]}"
                                            )
                                    send_telegram(
                                        "💰 <b>SALDO CEX</b>\n" +
                                        ("\n".join(lines) if lines else "• Tidak ada CEX aktif")
                                    )

                                threading.Thread(target=balance_worker, daemon=True).start()

                            except Exception as e:
                                send_telegram(
                                    f"⚠️ <b>POSISI ERROR</b>\n<code>{str(e)[:2000]}</code>"
                                )

                        threading.Thread(target=posisi_worker, daemon=True).start()
                    except Exception as e:
                        send_telegram(
                            f"⚠️ <b>POSISI ERROR</b>\n<code>{str(e)[:2000]}</code>"
                        )
                    continue

                # ---------------------------------------------------------
                # /setapi: save, validate asynchronously, disable on failure.
                # ---------------------------------------------------------
                if command == "/setapi":
                    if len(args) != 3:
                        send_telegram("⚠️ Format:\n<code>/setapi &lt;cex&gt; &lt;api_key&gt; &lt;secret&gt;</code>")
                        continue

                    ex_id, api, sec = args[0].lower(), args[1], args[2]
                    if ex_id not in DEFAULT_CEX:
                        send_telegram("❌ CEX tidak dikenal.")
                        continue

                    try:
                        conn = sqlite3.connect(DB_FILE, timeout=3)
                        conn.execute(
                            "UPDATE cex_settings SET api_key=?, secret=?, enabled=0 WHERE ex_id=?",
                            (api, sec, ex_id)
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        send_telegram(
                            f"❌ <b>GAGAL SIMPAN API {ex_id.upper()}</b>\n"
                            f"<code>{str(e)[:2000]}</code>"
                        )
                        continue

                    send_telegram(
                        f"🔎 <b>VALIDASI {ex_id.upper()}</b>\n"
                        "API disimpan sementara dan sedang dicek..."
                    )

                    def validate_worker(exchange_id):
                        ok, detail = validate_exchange_credentials(exchange_id)
                        if ok:
                            reload_exchanges()
                            send_telegram(
                                f"✅ <b>{exchange_id.upper()} API VALID</b>\n{detail}\n"
                                "CEX masih OFF. Gunakan /cex on setelah yakin."
                            )
                        else:
                            try:
                                conn = sqlite3.connect(DB_FILE, timeout=3)
                                conn.execute(
                                    "UPDATE cex_settings SET enabled=0 WHERE ex_id=?",
                                    (exchange_id,)
                                )
                                conn.commit()
                                conn.close()
                            except Exception:
                                pass

                            reload_exchanges()
                            send_telegram(
                                f"❌ <b>{exchange_id.upper()} API TIDAK VALID</b>\n"
                                f"{detail}\n\n"
                                "🔴 CEX otomatis tetap OFF."
                            )

                    threading.Thread(
                        target=validate_worker,
                        args=(ex_id,),
                        daemon=True
                    ).start()
                    continue

                # ---------------------------------------------------------
                # /cex on/off: never allow ON without successful validation.
                # ---------------------------------------------------------
                if command == "/cex":
                    if len(args) != 2:
                        send_telegram("⚠️ Format: <code>/cex on/off &lt;cex&gt;</code>")
                        continue

                    action, ex_id = args[0].lower(), args[1].lower()
                    if ex_id not in DEFAULT_CEX or action not in ("on", "off"):
                        send_telegram("⚠️ CEX/action tidak valid.")
                        continue

                    if action == "on":
                        send_telegram(f"🔎 Validasi {ex_id.upper()} sebelum ON...")
                        ok, detail = validate_exchange_credentials(ex_id)
                        if not ok:
                            send_telegram(
                                f"❌ <b>{ex_id.upper()} TIDAK BISA ON</b>\n{detail}\n"
                                "🔴 CEX tetap OFF."
                            )
                            continue

                    try:
                        conn = sqlite3.connect(DB_FILE, timeout=3)
                        conn.execute(
                            "UPDATE cex_settings SET enabled=? WHERE ex_id=?",
                            (1 if action == "on" else 0, ex_id)
                        )
                        conn.commit()
                        conn.close()
                        reload_exchanges()

                        send_telegram(
                            f"✅ CEX {ex_id.upper()} di-{action.upper()}!"
                            + (f"\n{detail}" if action == "on" else "")
                        )
                    except Exception as e:
                        send_telegram(
                            f"❌ Gagal ubah status CEX:\n<code>{str(e)[:2000]}</code>"
                        )
                    continue

                # ---------------------------------------------------------
                # Existing commands
                # ---------------------------------------------------------
                try:
                    if command == "/dryrun" and len(args) == 1:
                        if args[0].lower() == "on":
                            CONFIG["DRY_RUN"] = True
                            update_db_setting("dry_run", 1.0)
                            send_telegram("✅ <b>DRY RUN AKTIF</b>")
                        elif args[0].lower() == "off":
                            CONFIG["DRY_RUN"] = False
                            update_db_setting("dry_run", 0.0)
                            send_telegram("⚠️ <b>REAL MONEY AKTIF</b>")
                        else:
                            send_telegram("⚠️ Format: /dryrun on atau /dryrun off")

                    elif command == "/setmargin" and len(args) == 1:
                        value = float(args[0])
                        CONFIG["MARGIN_PER_TRADE"] = value
                        update_db_setting("margin", value)
                        send_telegram(f"✅ Margin: ${value}")

                    elif command == "/setmaxpos" and len(args) == 1:
                        value = int(args[0])
                        CONFIG["MAX_CONCURRENT_POSITIONS"] = value
                        update_db_setting("max_pos", value)
                        send_telegram(f"✅ Max Posisi: {value}")

                    elif command == "/setminwr" and len(args) == 1:
                        value = float(args[0])
                        CONFIG["MIN_WR"] = value
                        update_db_setting("min_wr", value)
                        send_telegram(f"✅ Min WinRate: {value}%")

                    elif command == "/setmintrades" and len(args) == 1:
                        value = int(args[0])
                        CONFIG["MIN_TRADES"] = value
                        update_db_setting("min_trades", value)
                        send_telegram(f"✅ Min Trade: {value}x")

                    elif command == "/setlev" and len(args) in (1, 2):
                        if len(args) == 1:
                            value = int(args[0])
                            for ex in DEFAULT_CEX:
                                CONFIG["LEVERAGE"][ex] = value
                                update_db_setting(f"lev_{ex}", value)
                            send_telegram(f"✅ Semua leverage: {value}x")
                        else:
                            ex_id, value = args[0].lower(), int(args[1])
                            if ex_id not in DEFAULT_CEX:
                                send_telegram("⚠️ CEX tidak dikenal.")
                            else:
                                CONFIG["LEVERAGE"][ex_id] = value
                                update_db_setting(f"lev_{ex_id}", value)
                                send_telegram(f"✅ Lev {ex_id.upper()}: {value}x")

                    elif command in ("/start", "/help"):
                        send_telegram(
                            "🤖 <b>COMMANDS</b>\n"
                            "• /cex on/off &lt;cex&gt; | /setapi &lt;cex&gt; &lt;api&gt; &lt;secret&gt;\n"
                            "• /dryrun on/off\n"
                            "• /posisi | /stat | /pairperf\n"
                            "• /setting | /settings\n"
                            "• /setmargin | /setlev | /setmaxpos\n"
                            "• /setminwr | /setmintrades\n"
                            "• /cancelall | /pause | /resume"
                        )

                    elif command == "/pause":
                        IS_BOT_PAUSED = True
                        send_telegram("⏸️ Bot PAUSED")

                    elif command == "/resume":
                        IS_BOT_PAUSED = False
                        send_telegram("▶️ Bot RESUMED")

                    elif command == "/cancelall":
                        cancel_all_pending_orders_on_pause()

                    elif command == "/stat":
                        conn = sqlite3.connect(DB_FILE, timeout=3)
                        rows = conn.execute(
                            "SELECT status, COUNT(*) FROM trades GROUP BY status"
                        ).fetchall()
                        conn.close()
                        stats = "\n".join(f"• {s}: {n}" for s, n in rows)
                        send_telegram(
                            f"📈 <b>STATISTIK</b>\n\n{stats or 'Belum ada data'}"
                        )

                    elif command == "/pairperf":
                        conn = sqlite3.connect(DB_FILE, timeout=3)
                        rows = conn.execute(
                            "SELECT symbol, status FROM trades "
                            "WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
                        ).fetchall()
                        conn.close()

                        perf = {}
                        for sym, stat in rows:
                            perf.setdefault(sym, {"w": 0, "l": 0})
                            perf[sym]["w" if "TP" in stat else "l"] += 1

                        lines = []
                        for sym, d in perf.items():
                            total = d["w"] + d["l"]
                            wr = d["w"] / total * 100 if total else 0
                            lines.append(
                                f"• {sym}: {d['w']}W/{d['l']}L (WR: {wr:.1f}%)"
                            )

                        send_telegram(
                            "🏆 <b>PERFORMA KOIN</b>\n" +
                            ("\n".join(lines) if lines else "Belum ada")
                        )

                    else:
                        send_telegram(
                            f"⚠️ Command tidak dikenal:\n<code>{raw[:200]}</code>"
                        )

                except Exception as cmd_error:
                    print(f"[COMMAND ERROR] {raw}: {cmd_error}", flush=True)
                    send_telegram(
                        "⚠️ <b>COMMAND ERROR</b>\n"
                        f"Command: <code>{raw[:200]}</code>\n"
                        f"<code>{str(cmd_error)[:2500]}</code>"
                    )

        except Exception as poll_error:
            print(f"[POLLING ERROR] {poll_error}", flush=True)
            time.sleep(3)

        time.sleep(0.3)


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
    send_telegram("🚀 <b>ALCHEMIST v2 INITIALIZED</b>\n🔧 Telegram Command Engine: FINAL")

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
