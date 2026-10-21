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

MANUAL_ALERT_PAIRS = [
    "XAU/USDT",
    "EUR/USDT",
    "GBP/USDT",
    "USOIL/USDT",
    "WTI/USDT",
    "USO/USDT"
]

CORE_PAIRS = [
    "BTC/USDT",
    "ETH/USDT"
]

DEFAULT_CEX = {
    "bingx": {"type": "swap"},
    "bitget": {"type": "swap"},
    "bybit": {"type": "linear"},
    "binance": {"type": "future"}
}

exchanges = {}

CONFIG = {
    "MARGIN_PER_TRADE": 2.0,
    "MAX_CONCURRENT_POSITIONS": 2,
    "LEVERAGE": {
        ex: 50 for ex in DEFAULT_CEX.keys()
    },
    "DRY_RUN": True,
    "MIN_TRADES": 5,
    "MIN_WR": 40.0
}

IS_BOT_PAUSED = False

daily_sl_count = 0

last_error_time = 0

ERROR_COOLDOWN = 300


# =================================================================
# DATABASE
# =================================================================

def init_db():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
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
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS cex_settings (
            ex_id TEXT PRIMARY KEY,
            api_key TEXT,
            secret TEXT,
            enabled INTEGER DEFAULT 0
        )
    """)

    # -------------------------------------------------------------
    # Default settings
    # -------------------------------------------------------------

    defaults = {
        "margin": 2.0,
        "max_pos": 2.0,
        "dry_run": 1.0,
        "min_trades": 5.0,
        "min_wr": 40.0
    }

    for key, value in defaults.items():

        c.execute(
            """
            INSERT OR IGNORE INTO settings
            (key, value)
            VALUES (?, ?)
            """,
            (key, value)
        )

    # -------------------------------------------------------------
    # CEX settings
    # -------------------------------------------------------------

    for ex in DEFAULT_CEX.keys():

        c.execute(
            """
            INSERT OR IGNORE INTO settings
            (key, value)
            VALUES (?, ?)
            """,
            (
                f"lev_{ex}",
                50.0
            )
        )

        c.execute(
            """
            INSERT OR IGNORE INTO cex_settings
            (ex_id, api_key, secret, enabled)
            VALUES (?, '', '', 0)
            """,
            (ex,)
        )

    conn.commit()
    conn.close()


def load_settings():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    c = conn.cursor()

    rows = c.execute(
        "SELECT key, value FROM settings"
    ).fetchall()

    conn.close()

    for key, value in rows:

        if key == "margin":

            CONFIG["MARGIN_PER_TRADE"] = float(value)

        elif key == "max_pos":

            CONFIG["MAX_CONCURRENT_POSITIONS"] = int(
                value
            )

        elif key == "dry_run":

            CONFIG["DRY_RUN"] = bool(value)

        elif key == "min_trades":

            CONFIG["MIN_TRADES"] = int(value)

        elif key == "min_wr":

            CONFIG["MIN_WR"] = float(value)

        elif key.startswith("lev_"):

            ex_id = key.split("_", 1)[1]

            CONFIG["LEVERAGE"][ex_id] = int(
                value
            )


def reload_exchanges():

    global exchanges

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    rows = conn.cursor().execute(
        """
        SELECT ex_id, api_key, secret, enabled
        FROM cex_settings
        """
    ).fetchall()

    conn.close()

    exchanges.clear()

    for ex_id, api, sec, enabled in rows:

        if enabled != 1:
            continue

        if ex_id not in DEFAULT_CEX:
            continue

        try:

            exchanges[ex_id] = getattr(
                ccxt,
                ex_id
            )({
                "apiKey": api or "",
                "secret": sec or "",
                "options": {
                    "defaultType":
                        DEFAULT_CEX[ex_id]["type"]
                },
                "enableRateLimit": True,
                "timeout": 5000
            })

            print(
                f"[CEX] {ex_id.upper()} loaded",
                flush=True
            )

        except Exception as e:

            print(
                f"[CEX LOAD ERROR] "
                f"{ex_id}: {e}",
                flush=True
            )


def update_db_setting(key, value):

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    conn.execute(
        """
        UPDATE settings
        SET value=?
        WHERE key=?
        """,
        (
            value,
            key
        )
    )

    conn.commit()
    conn.close()


# =================================================================
# TELEGRAM
# =================================================================

def send_telegram(message):

    try:

        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        )

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }

        response = requests.post(
            url,
            json=payload,
            timeout=10
        )

        if response.status_code != 200:

            print(
                f"[TELEGRAM SEND ERROR] "
                f"{response.text}",
                flush=True
            )

            payload.pop(
                "parse_mode",
                None
            )

            requests.post(
                url,
                json=payload,
                timeout=10
            )

    except Exception as e:

        print(
            f"[TELEGRAM SEND ERROR] {e}",
            flush=True
        )


def send_error_alert(err_msg):

    global last_error_time

    if (
        time.time() - last_error_time
        > ERROR_COOLDOWN
    ):

        send_telegram(
            "⚠️ <b>ERROR ALERT</b>\n\n"
            f"<code>{str(err_msg)}</code>"
        )

        last_error_time = time.time()


# =================================================================
# HELPERS
# =================================================================

def is_kill_zone():

    h = datetime.now(WIB).hour

    return (
        (14 <= h < 17)
        or
        (19 <= h < 22)
    )


def is_manual_alert_symbol(symbol):

    clean_symbol = symbol.split(":")[0].upper()

    return any(
        pair in clean_symbol
        for pair in MANUAL_ALERT_PAIRS
    )


def get_free_usdt_balance(ex_instance):

    try:

        balance = ex_instance.fetch_balance()

        usdt = balance.get(
            "USDT",
            {}
        )

        if isinstance(usdt, dict):

            return float(
                usdt.get(
                    "free",
                    0.0
                )
            )

        return 0.0

    except Exception as e:

        print(
            f"[BALANCE ERROR] {e}",
            flush=True
        )

        return 0.0


def cancel_all_pending_orders_on_pause():

    try:

        conn = sqlite3.connect(
            DB_FILE,
            timeout=10
        )

        c = conn.cursor()

        trades = c.execute(
            """
            SELECT
                id,
                exchange_id,
                symbol,
                order_id
            FROM trades
            WHERE status='PENDING'
            """
        ).fetchall()

        for (
            trade_id,
            ex_id,
            symbol,
            order_id
        ) in trades:

            if (
                ex_id in exchanges
                and not str(order_id).startswith("sim_")
            ):

                try:

                    exchanges[
                        ex_id
                    ].cancel_order(
                        order_id,
                        symbol
                    )

                except Exception as e:

                    print(
                        f"[CANCEL ERROR] "
                        f"{ex_id} {symbol}: {e}",
                        flush=True
                    )

            c.execute(
                """
                UPDATE trades
                SET status=?
                WHERE id=?
                """,
                (
                    "CANCELLED (Auto/Manual)",
                    trade_id
                )
            )

        conn.commit()
        conn.close()

        send_telegram(
            "🧹 <b>CLEANUP</b>\n"
            "Semua pending order dibatalkan!"
        )

    except Exception as e:

        print(
            f"[CANCEL ALL ERROR] {e}",
            flush=True
        )

        send_error_alert(
            f"CancelAll: {e}"
        )


def check_and_update_daily_sl():

    global daily_sl_count
    global IS_BOT_PAUSED

    try:

        conn = sqlite3.connect(
            DB_FILE,
            timeout=10
        )

        # ---------------------------------------------------------
        # Gunakan WIB untuk daily SL
        # ---------------------------------------------------------

        now_wib = datetime.now(WIB)

        start_wib = datetime(
            now_wib.year,
            now_wib.month,
            now_wib.day,
            tzinfo=WIB
        )

        start_utc = (
            start_wib
            .astimezone(timezone.utc)
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        count = conn.cursor().execute(
            """
            SELECT COUNT(*)
            FROM trades
            WHERE created_at >= ?
            AND status LIKE '%SL%'
            """,
            (start_utc,)
        ).fetchone()[0]

        conn.close()

        daily_sl_count = count

        if (
            daily_sl_count >= MAX_DAILY_SL
            and not IS_BOT_PAUSED
        ):

            IS_BOT_PAUSED = True

            cancel_all_pending_orders_on_pause()

            send_telegram(
                "🛑 <b>DAILY LOSS LIMIT</b>\n"
                f"Kena {daily_sl_count}x SL.\n"
                "Bot otomatis PAUSE!"
            )

    except Exception as e:

        print(
            f"[DAILY SL ERROR] {e}",
            flush=True
        )


# =================================================================
# SMC STRATEGY
# =================================================================

def analyze_smc_retrace(
    df_m15,
    df_h1,
    daily_open
):

    try:

        ema50 = (
            df_h1["close"]
            .ewm(
                span=50,
                adjust=False
            )
            .mean()
            .iloc[-2]
        )

        h1_close = df_h1[
            "close"
        ].iloc[-2]

        c_prev = df_m15.iloc[-2]

        sig = None

        entry = 0.0
        sl = 0.0
        tp = 0.0

        # ---------------------------------------------------------
        # BUY
        # ---------------------------------------------------------

        if (
            h1_close > ema50
            and c_prev["close"] > daily_open
        ):

            previous_lows = df_m15[
                "low"
            ].iloc[-7:-2]

            if (
                len(previous_lows) > 0
                and
                c_prev["low"]
                < previous_lows.min()
                and
                c_prev["close"]
                > c_prev["open"]
            ):

                entry = (
                    c_prev["high"]
                    -
                    (
                        0.5
                        *
                        (
                            c_prev["high"]
                            -
                            c_prev["low"]
                        )
                    )
                )

                sl = (
                    c_prev["low"]
                    *
                    (
                        1.0
                        -
                        SL_BUFFER_PERCENT
                    )
                )

                tp = (
                    entry
                    +
                    (
                        entry - sl
                    )
                )

                sig = "BUY"

        # ---------------------------------------------------------
        # SELL
        # ---------------------------------------------------------

        elif (
            h1_close < ema50
            and c_prev["close"] < daily_open
        ):

            previous_highs = df_m15[
                "high"
            ].iloc[-7:-2]

            if (
                len(previous_highs) > 0
                and
                c_prev["high"]
                > previous_highs.max()
                and
                c_prev["close"]
                < c_prev["open"]
            ):

                entry = (
                    c_prev["low"]
                    +
                    (
                        0.5
                        *
                        (
                            c_prev["high"]
                            -
                            c_prev["low"]
                        )
                    )
                )

                sl = (
                    c_prev["high"]
                    *
                    (
                        1.0
                        +
                        SL_BUFFER_PERCENT
                    )
                )

                tp = (
                    entry
                    -
                    (
                        sl - entry
                    )
                )

                sig = "SELL"

        return sig, entry, sl, tp

    except Exception as e:

        print(
            f"[SMC ERROR] {e}",
            flush=True
        )

        return None, 0.0, 0.0, 0.0


# =================================================================
# ORDER & POSITION UPDATE
# =================================================================

def update_orders_and_positions():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    c = conn.cursor()

    trades = c.execute(
        """
        SELECT
            id,
            exchange_id,
            symbol,
            side,
            entry_price,
            sl_price,
            tp_price,
            order_id,
            bars_count,
            status
        FROM trades
        WHERE status IN ('PENDING', 'OPEN')
        """
    ).fetchall()

    for (
        trade_id,
        ex_id,
        symbol,
        side,
        entry,
        sl,
        tp,
        order_id,
        bars,
        status
    ) in trades:

        if ex_id not in exchanges:
            continue

        try:

            ticker = exchanges[
                ex_id
            ].fetch_ticker(symbol)

            curr_price = ticker.get("last")

            if curr_price is None:
                continue

            n_bars = bars + 1

            is_sim = str(
                order_id
            ).startswith("sim_")

            tag = (
                " (DRY RUN)"
                if is_sim
                else ""
            )

            # -----------------------------------------------------
            # PENDING
            # -----------------------------------------------------

            if status == "PENDING":

                new_status = "PENDING"

                # BUY filled
                if (
                    side == "BUY"
                    and curr_price <= entry
                ):

                    new_status = "OPEN"

                    send_telegram(
                        f"🔔 <b>ORDER FILLED"
                        f"{tag}</b>\n"
                        f"{symbol} di "
                        f"{ex_id.upper()}!"
                    )

                # SELL filled
                elif (
                    side == "SELL"
                    and curr_price >= entry
                ):

                    new_status = "OPEN"

                    send_telegram(
                        f"🔔 <b>ORDER FILLED"
                        f"{tag}</b>\n"
                        f"{symbol} di "
                        f"{ex_id.upper()}!"
                    )

                # BUY invalid
                elif side == "BUY":

                    if curr_price >= tp:

                        new_status = (
                            "MISSED (TP First)"
                        )

                    elif curr_price <= sl:

                        new_status = (
                            "CANCELLED (SL First)"
                        )

                    elif n_bars > 30:

                        new_status = (
                            "INVALID (>30 Bars)"
                        )

                # SELL invalid
                elif side == "SELL":

                    if curr_price <= tp:

                        new_status = (
                            "MISSED (TP First)"
                        )

                    elif curr_price >= sl:

                        new_status = (
                            "CANCELLED (SL First)"
                        )

                    elif n_bars > 30:

                        new_status = (
                            "INVALID (>30 Bars)"
                        )

                if new_status != "PENDING":

                    if (
                        (
                            "CANCEL" in new_status
                            or
                            "MISS" in new_status
                            or
                            "INV" in new_status
                        )
                        and
                        not is_sim
                    ):

                        try:

                            exchanges[
                                ex_id
                            ].cancel_order(
                                order_id,
                                symbol
                            )

                        except Exception:
                            pass

                    send_telegram(
                        f"⚪ <b>AUTO-CANCEL"
                        f"{tag}</b>\n"
                        f"{symbol}\n"
                        f"Alasan: {new_status}"
                    )

                    c.execute(
                        """
                        UPDATE trades
                        SET status=?,
                            bars_count=?
                        WHERE id=?
                        """,
                        (
                            new_status,
                            n_bars,
                            trade_id
                        )
                    )

                else:

                    c.execute(
                        """
                        UPDATE trades
                        SET bars_count=?
                        WHERE id=?
                        """,
                        (
                            n_bars,
                            trade_id
                        )
                    )

            # -----------------------------------------------------
            # OPEN
            # -----------------------------------------------------

            elif status == "OPEN":

                # TP
                if (
                    (
                        side == "BUY"
                        and curr_price >= tp
                    )
                    or
                    (
                        side == "SELL"
                        and curr_price <= tp
                    )
                ):

                    c.execute(
                        """
                        UPDATE trades
                        SET status='CLOSED (TP)'
                        WHERE id=?
                        """,
                        (trade_id,)
                    )

                    send_telegram(
                        f"🎉 <b>TAKE PROFIT"
                        f"{tag}</b>\n"
                        f"{symbol} | +1R 🚀"
                    )

                # SL
                elif (
                    (
                        side == "BUY"
                        and curr_price <= sl
                    )
                    or
                    (
                        side == "SELL"
                        and curr_price >= sl
                    )
                ):

                    c.execute(
                        """
                        UPDATE trades
                        SET status='CLOSED (SL)'
                        WHERE id=?
                        """,
                        (trade_id,)
                    )

                    send_telegram(
                        f"❌ <b>STOP LOSS"
                        f"{tag}</b>\n"
                        f"{symbol} | -1R 🔻"
                    )

        except Exception as e:

            print(
                f"[POSITION UPDATE ERROR] "
                f"{ex_id} {symbol}: {e}",
                flush=True
            )

    conn.commit()
    conn.close()


# =================================================================
# MARKET SCANNER
# =================================================================

def scan_markets():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    c = conn.cursor()

    act_count = c.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE status IN ('PENDING', 'OPEN')
        """
    ).fetchone()[0]

    pair_perf = {}

    rows = c.execute(
        """
        SELECT symbol, status
        FROM trades
        WHERE status IN
        ('CLOSED (TP)', 'CLOSED (SL)')
        """
    ).fetchall()

    conn.close()

    for symbol, status in rows:

        if symbol not in pair_perf:

            pair_perf[symbol] = {
                "w": 0,
                "l": 0
            }

        if "TP" in status:

            pair_perf[symbol]["w"] += 1

        else:

            pair_perf[symbol]["l"] += 1

    for ex_id, ex in exchanges.items():

        try:

            tickers = ex.fetch_tickers()

            symbols = [
                s
                for s in tickers.keys()
                if (
                    s.endswith("/USDT:USDT")
                    or
                    s.endswith("/USDT")
                )
            ]

            # Limit scanner
            symbols = symbols[:60]

            for symbol in symbols:

                # -------------------------------------------------
                # Manual alert pair
                # -------------------------------------------------

                if is_manual_alert_symbol(
                    symbol
                ):

                    try:

                        m15 = ex.fetch_ohlcv(
                            symbol,
                            "15m",
                            limit=50
                        )

                        h1 = ex.fetch_ohlcv(
                            symbol,
                            "1h",
                            limit=60
                        )

                        d1 = ex.fetch_ohlcv(
                            symbol,
                            "1d",
                            limit=2
                        )

                        if (
                            not m15
                            or not h1
                            or not d1
                        ):
                            continue

                        m15_df = pd.DataFrame(
                            m15,
                            columns=[
                                "time",
                                "open",
                                "high",
                                "low",
                                "close",
                                "vol"
                            ]
                        )

                        h1_df = pd.DataFrame(
                            h1,
                            columns=[
                                "time",
                                "open",
                                "high",
                                "low",
                                "close",
                                "vol"
                            ]
                        )

                        sig, entry, sl, tp = (
                            analyze_smc_retrace(
                                m15_df,
                                h1_df,
                                d1[-1][1]
                            )
                        )

                        if sig:

                            send_telegram(
                                "📢 <b>FX ALERT</b>\n"
                                f"{symbol}\n"
                                f"Signal: {sig}\n"
                                f"Entry: {entry:.5f}\n"
                                f"SL: {sl:.5f}\n"
                                f"TP: {tp:.5f}"
                            )

                    except Exception as e:

                        print(
                            f"[MANUAL ALERT ERROR] "
                            f"{symbol}: {e}",
                            flush=True
                        )

                    continue

                # -------------------------------------------------
                # Position limit
                # -------------------------------------------------

                if (
                    act_count
                    >=
                    CONFIG[
                        "MAX_CONCURRENT_POSITIONS"
                    ]
                ):
                    continue

                # -------------------------------------------------
                # Pair performance filter
                # -------------------------------------------------

                if (
                    symbol in pair_perf
                    and
                    symbol not in CORE_PAIRS
                ):

                    total_trades = (
                        pair_perf[symbol]["w"]
                        +
                        pair_perf[symbol]["l"]
                    )

                    if (
                        total_trades
                        >= CONFIG["MIN_TRADES"]
                    ):

                        wr = (
                            pair_perf[symbol]["w"]
                            /
                            total_trades
                        ) * 100

                        if wr < CONFIG["MIN_WR"]:
                            continue

                # -------------------------------------------------
                # OHLCV
                # -------------------------------------------------

                m15 = ex.fetch_ohlcv(
                    symbol,
                    "15m",
                    limit=50
                )

                h1 = ex.fetch_ohlcv(
                    symbol,
                    "1h",
                    limit=60
                )

                d1 = ex.fetch_ohlcv(
                    symbol,
                    "1d",
                    limit=2
                )

                if (
                    not m15
                    or not h1
                    or not d1
                ):
                    continue

                m15_df = pd.DataFrame(
                    m15,
                    columns=[
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol"
                    ]
                )

                h1_df = pd.DataFrame(
                    h1,
                    columns=[
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol"
                    ]
                )

                sig, entry, sl, tp = (
                    analyze_smc_retrace(
                        m15_df,
                        h1_df,
                        d1[-1][1]
                    )
                )

                if not sig:
                    continue

                # -------------------------------------------------
                # Prevent duplicate active pair
                # -------------------------------------------------

                conn = sqlite3.connect(
                    DB_FILE,
                    timeout=10
                )

                duplicate = conn.cursor().execute(
                    """
                    SELECT COUNT(*)
                    FROM trades
                    WHERE exchange_id=?
                    AND symbol=?
                    AND status IN
                    ('PENDING', 'OPEN')
                    """,
                    (
                        ex_id,
                        symbol
                    )
                ).fetchone()[0]

                conn.close()

                if duplicate > 0:
                    continue

                # -------------------------------------------------
                # Position calculation
                # -------------------------------------------------

                leverage = CONFIG[
                    "LEVERAGE"
                ].get(
                    ex_id,
                    50
                )

                if entry <= 0:
                    continue

                qty = (
                    CONFIG[
                        "MARGIN_PER_TRADE"
                    ]
                    *
                    leverage
                ) / entry

                mode = (
                    "[DRY RUN]"
                    if CONFIG["DRY_RUN"]
                    else "[REAL TRADE]"
                )

                order_id = (
                    f"sim_"
                    f"{int(time.time() * 1000)}"
                )

                # -------------------------------------------------
                # REAL ORDER
                # -------------------------------------------------

                if not CONFIG["DRY_RUN"]:

                    try:

                        try:

                            ex.set_leverage(
                                leverage,
                                symbol
                            )

                        except Exception:
                            pass

                        order_side = (
                            "buy"
                            if sig == "BUY"
                            else "sell"
                        )

                        order = ex.create_order(
                            symbol,
                            "limit",
                            order_side,
                            qty,
                            entry,
                            params={
                                "stopLoss": {
                                    "price": sl
                                },
                                "takeProfit": {
                                    "price": tp
                                }
                            }
                        )

                        order_id = str(
                            order["id"]
                        )

                    except Exception as e:

                        send_error_alert(
                            f"Order Failed "
                            f"{symbol}: {e}"
                        )

                        continue

                # -------------------------------------------------
                # Save trade
                # -------------------------------------------------

                conn = sqlite3.connect(
                    DB_FILE,
                    timeout=10
                )

                conn.execute(
                    """
                    INSERT INTO trades (
                        exchange_id,
                        symbol,
                        order_type,
                        side,
                        entry_price,
                        sl_price,
                        tp_price,
                        order_id,
                        status
                    )
                    VALUES (
                        ?, ?, 'LIMIT', ?,
                        ?, ?, ?, ?, 'PENDING'
                    )
                    """,
                    (
                        ex_id,
                        symbol,
                        sig,
                        entry,
                        sl,
                        tp,
                        order_id
                    )
                )

                conn.commit()
                conn.close()

                send_telegram(
                    f"🎯 <b>{mode}</b>\n"
                    f"CEX: {ex_id.upper()}\n"
                    f"Pair: {symbol}\n"
                    f"Signal: {sig}\n"
                    f"Entry: {entry:.5f}\n"
                    f"SL: {sl:.5f}\n"
                    f"TP: {tp:.5f}\n"
                    f"Lev: {leverage}x"
                )

                act_count += 1

        except Exception as e:

            print(
                f"[SCAN ERROR] "
                f"{ex_id}: {e}",
                flush=True
            )

            send_error_alert(
                f"Scan Ex "
                f"[{ex_id}]: {e}"
            )


# =================================================================
# TELEGRAM COMMAND PROCESSOR
# =================================================================

def process_telegram_update(update):

    global IS_BOT_PAUSED

    try:

        msg = update.get(
            "message",
            {}
        )

        chat_id = str(
            msg.get(
                "chat",
                {}
            ).get(
                "id",
                ""
            )
        )

        if chat_id != str(
            TELEGRAM_CHAT_ID
        ):
            return

        text = str(
            msg.get(
                "text",
                ""
            )
        ).strip()

        if not text:
            return

        parts = text.split()

        command = parts[0].lower()

        print(
            f"[TELEGRAM] {text}",
            flush=True
        )

        # =========================================================
        # HELP
        # =========================================================

        if command in [
            "/start",
            "/help"
        ]:

            send_telegram(
                "🤖 <b>[COMMANDS]</b>\n\n"
                "• /cex on/off &lt;cex&gt;\n"
                "• /setapi &lt;cex&gt; &lt;api&gt; &lt;secret&gt;\n"
                "• /dryrun on/off\n"
                "• /posisi\n"
                "• /settings\n"
                "• /stat\n"
                "• /pairperf\n"
                "• /setmargin &lt;value&gt;\n"
                "• /setlev &lt;value&gt;\n"
                "• /setmaxpos &lt;value&gt;\n"
                "• /setminwr &lt;value&gt;\n"
                "• /setmintrades &lt;value&gt;\n"
                "• /cancelall\n"
                "• /pause\n"
                "• /resume"
            )

            return

        # =========================================================
        # SETTINGS
        # =========================================================

        elif command in [
            "/setting",
            "/settings"
        ]:

            try:

                conn = sqlite3.connect(
                    DB_FILE,
                    timeout=5
                )

                rows = conn.execute(
                    """
                    SELECT
                        ex_id,
                        api_key,
                        enabled
                    FROM cex_settings
                    ORDER BY ex_id
                    """
                ).fetchall()

                conn.close()

                mode_text = (
                    "🟢 DRY RUN"
                    if CONFIG["DRY_RUN"]
                    else "⚠️ REAL MONEY"
                )

                cex_info = ""

                for (
                    ex_id,
                    api_key,
                    enabled
                ) in rows:

                    status = (
                        "🟢 ON"
                        if enabled
                        else "🔴 OFF"
                    )

                    api_status = (
                        "✅"
                        if api_key
                        else "❌"
                    )

                    lev = CONFIG[
                        "LEVERAGE"
                    ].get(
                        ex_id,
                        50
                    )

                    cex_info += (
                        f"• <b>{ex_id.upper()}</b>: "
                        f"{status} | "
                        f"Lev: {lev}x | "
                        f"API: {api_status}\n"
                    )

                if not cex_info:
                    cex_info = "Belum ada CEX.\n"

                message = (
                    "⚙️ <b>SETTING BOT</b>\n\n"
                    f"Mode: {mode_text}\n"
                    f"Margin: "
                    f"${CONFIG['MARGIN_PER_TRADE']:.2f}\n"
                    f"Max Pos: "
                    f"{CONFIG['MAX_CONCURRENT_POSITIONS']}\n"
                    f"Min WR: "
                    f"{CONFIG['MIN_WR']:.1f}%\n"
                    f"Min Trade: "
                    f"{CONFIG['MIN_TRADES']}x\n\n"
                    "🏦 <b>BURSA</b>\n"
                    f"{cex_info}"
                )

                send_telegram(
                    message
                )

            except Exception as e:

                print(
                    f"[SETTINGS ERROR] {e}",
                    flush=True
                )

                send_telegram(
                    "⚠️ <b>SETTINGS ERROR</b>\n"
                    f"<code>{e}</code>"
                )

            return

        # =========================================================
        # POSISI
        # =========================================================

        elif command == "/posisi":

            try:

                conn = sqlite3.connect(
                    DB_FILE,
                    timeout=5
                )

                trades = conn.execute(
                    """
                    SELECT
                        exchange_id,
                        symbol,
                        side,
                        status,
                        entry_price,
                        sl_price,
                        tp_price
                    FROM trades
                    WHERE status IN
                    ('PENDING', 'OPEN')
                    ORDER BY id DESC
                    """
                ).fetchall()

                cex_db = conn.execute(
                    """
                    SELECT
                        ex_id,
                        enabled
                    FROM cex_settings
                    ORDER BY ex_id
                    """
                ).fetchall()

                conn.close()

                mode = (
                    "🟢 DRY RUN"
                    if CONFIG["DRY_RUN"]
                    else "⚠️ REAL MONEY"
                )

                bot_status = (
                    "⏸️ PAUSED"
                    if IS_BOT_PAUSED
                    else "🟢 RUNNING"
                )

                cex_status = ""

                for (
                    ex_id,
                    enabled
                ) in cex_db:

                    status = (
                        "🟢 ON"
                        if enabled
                        else "🔴 OFF"
                    )

                    cex_status += (
                        f"• {ex_id.upper()} "
                        f"{status}\n"
                    )

                if not cex_status:

                    cex_status = (
                        "Tidak ada CEX.\n"
                    )

                message = (
                    "📊 <b>STATUS & POSISI</b>\n\n"
                    f"Bot: {bot_status}\n"
                    f"Mode: {mode}\n\n"
                    "🔌 <b>CEX:</b>\n"
                    f"{cex_status}\n"
                    f"📋 <b>ANTREAN:</b> "
                    f"{len(trades)}\n\n"
                )

                if not trades:

                    message += (
                        "✅ Tidak ada posisi aktif."
                    )

                else:

                    for trade in trades:

                        (
                            ex_id,
                            symbol,
                            side,
                            status,
                            entry,
                            sl,
                            tp
                        ) = trade

                        message += (
                            f"• <b>{ex_id.upper()}</b> | "
                            f"{symbol}\n"
                            f"  {side} | {status}\n"
                            f"  Entry: {entry:.6f}\n"
                            f"  SL: {sl:.6f}\n"
                            f"  TP: {tp:.6f}\n\n"
                        )

                # -------------------------------------------------
                # IMPORTANT:
                # Tidak fetch_balance di sini.
                # Jadi /posisi tidak akan hang karena CEX timeout.
                # -------------------------------------------------

                send_telegram(
                    message
                )

            except Exception as e:

                print(
                    f"[POSISI ERROR] {e}",
                    flush=True
                )

                send_telegram(
                    "⚠️ <b>POSISI ERROR</b>\n"
                    f"<code>{e}</code>"
                )

            return

        # =========================================================
        # SET API
        # =========================================================

        elif command == "/setapi":

            if len(parts) != 4:

                send_telegram(
                    "❌ Format:\n"
                    "<code>/setapi "
                    "cex api secret</code>"
                )

                return

            ex_id = parts[1].lower()

            api = parts[2]

            secret = parts[3]

            if ex_id not in DEFAULT_CEX:

                send_telegram(
                    f"❌ CEX "
                    f"<b>{ex_id}</b> "
                    f"tidak tersedia."
                )

                return

            conn = sqlite3.connect(
                DB_FILE,
                timeout=5
            )

            conn.execute(
                """
                UPDATE cex_settings
                SET api_key=?,
                    secret=?
                WHERE ex_id=?
                """,
                (
                    api,
                    secret,
                    ex_id
                )
            )

            conn.commit()
            conn.close()

            reload_exchanges()

            send_telegram(
                f"✅ API Key "
                f"<b>{ex_id.upper()}</b> "
                f"disimpan!"
            )

            return

        # =========================================================
        # CEX
        # =========================================================

        elif command == "/cex":

            if len(parts) != 3:

                send_telegram(
                    "❌ Format:\n"
                    "<code>/cex on binance</code>"
                )

                return

            action = parts[1].lower()

            ex_id = parts[2].lower()

            if (
                ex_id not in DEFAULT_CEX
                or action not in [
                    "on",
                    "off"
                ]
            ):

                send_telegram(
                    "❌ CEX atau action "
                    "tidak valid."
                )

                return

            enabled = (
                1
                if action == "on"
                else 0
            )

            conn = sqlite3.connect(
                DB_FILE,
                timeout=5
            )

            conn.execute(
                """
                UPDATE cex_settings
                SET enabled=?
                WHERE ex_id=?
                """,
                (
                    enabled,
                    ex_id
                )
            )

            conn.commit()
            conn.close()

            reload_exchanges()

            send_telegram(
                f"✅ CEX "
                f"<b>{ex_id.upper()}</b> "
                f"di-{action.upper()}!"
            )

            return

        # =========================================================
        # DRY RUN
        # =========================================================

        elif command == "/dryrun":

            if len(parts) != 2:

                send_telegram(
                    "❌ Gunakan:\n"
                    "<code>/dryrun on</code>\n"
                    "<code>/dryrun off</code>"
                )

                return

            mode = parts[1].lower()

            if mode == "on":

                CONFIG["DRY_RUN"] = True

                update_db_setting(
                    "dry_run",
                    1.0
                )

                send_telegram(
                    "✅ <b>DRY RUN AKTIF</b>"
                )

            elif mode == "off":

                CONFIG["DRY_RUN"] = False

                update_db_setting(
                    "dry_run",
                    0.0
                )

                send_telegram(
                    "⚠️ <b>REAL MONEY AKTIF</b>"
                )

            else:

                send_telegram(
                    "❌ Gunakan on atau off."
                )

            return

        # =========================================================
        # SET MARGIN
        # =========================================================

        elif command == "/setmargin":

            if len(parts) != 2:

                send_telegram(
                    "❌ Contoh:\n"
                    "<code>/setmargin 2</code>"
                )

                return

            value = float(parts[1])

            if value <= 0:

                raise ValueError(
                    "Margin harus > 0."
                )

            CONFIG[
                "MARGIN_PER_TRADE"
            ] = value

            update_db_setting(
                "margin",
                value
            )

            send_telegram(
                f"✅ Margin: "
                f"${value:.2f}"
            )

            return

        # =========================================================
        # SET MAX POSITION
        # =========================================================

        elif command == "/setmaxpos":

            if len(parts) != 2:

                send_telegram(
                    "❌ Contoh:\n"
                    "<code>/setmaxpos 2</code>"
                )

                return

            value = int(parts[1])

            if value < 1:

                raise ValueError(
                    "Max posisi minimal 1."
                )

            CONFIG[
                "MAX_CONCURRENT_POSITIONS"
            ] = value

            update_db_setting(
                "max_pos",
                value
            )

            send_telegram(
                f"✅ Max Posisi: "
                f"{value}"
            )

            return

        # =========================================================
        # SET MIN WR
        # =========================================================

        elif command == "/setminwr":

            if len(parts) != 2:

                send_telegram(
                    "❌ Contoh:\n"
                    "<code>/setminwr 40</code>"
                )

                return

            value = float(parts[1])

            if value < 0 or value > 100:

                raise ValueError(
                    "WR harus 0-100."
                )

            CONFIG[
                "MIN_WR"
            ] = value

            update_db_setting(
                "min_wr",
                value
            )

            send_telegram(
                f"✅ Min WinRate: "
                f"{value:.1f}%"
            )

            return

        # =========================================================
        # SET MIN TRADES
        # =========================================================

        elif command == "/setmintrades":

            if len(parts) != 2:

                send_telegram(
                    "❌ Contoh:\n"
                    "<code>/setmintrades 5</code>"
                )

                return

            value = int(parts[1])

            if value < 0:

                raise ValueError(
                    "Min trade tidak boleh negatif."
                )

            CONFIG[
                "MIN_TRADES"
            ] = value

            update_db_setting(
                "min_trades",
                value
            )

            send_telegram(
                f"✅ Min Trade: "
                f"{value}x"
            )

            return

        # =========================================================
        # SET LEVERAGE
        # =========================================================

        elif command == "/setlev":

            # /setlev 50
            if len(parts) == 2:

                value = int(parts[1])

                if value <= 0:

                    raise ValueError(
                        "Leverage harus > 0."
                    )

                for ex_id in DEFAULT_CEX:

                    CONFIG[
                        "LEVERAGE"
                    ][ex_id] = value

                    update_db_setting(
                        f"lev_{ex_id}",
                        value
                    )

                send_telegram(
                    f"✅ Semua leverage: "
                    f"{value}x"
                )

                return

            # /setlev bybit 50
            elif len(parts) == 3:

                ex_id = parts[1].lower()

                value = int(parts[2])

                if ex_id not in DEFAULT_CEX:

                    send_telegram(
                        f"❌ CEX "
                        f"{ex_id} tidak tersedia."
                    )

                    return

                if value <= 0:

                    raise ValueError(
                        "Leverage harus > 0."
                    )

                CONFIG[
                    "LEVERAGE"
                ][ex_id] = value

                update_db_setting(
                    f"lev_{ex_id}",
                    value
                )

                send_telegram(
                    f"✅ Lev "
                    f"{ex_id.upper()}: "
                    f"{value}x"
                )

                return

            send_telegram(
                "❌ Format:\n"
                "<code>/setlev 50</code>\n"
                "atau\n"
                "<code>/setlev "
                "bybit 50</code>"
            )

            return

        # =========================================================
        # PAIR PERFORMANCE
        # =========================================================

        elif command == "/pairperf":

            conn = sqlite3.connect(
                DB_FILE,
                timeout=5
            )

            rows = conn.execute(
                """
                SELECT symbol, status
                FROM trades
                WHERE status IN
                ('CLOSED (TP)', 'CLOSED (SL)')
                """
            ).fetchall()

            conn.close()

            pair_perf = {}

            for symbol, status in rows:

                if symbol not in pair_perf:

                    pair_perf[symbol] = {
                        "w": 0,
                        "l": 0
                    }

                if "TP" in status:

                    pair_perf[
                        symbol
                    ]["w"] += 1

                else:

                    pair_perf[
                        symbol
                    ]["l"] += 1

            active = ""
            waiting = ""
            blacklist = ""

            for symbol, data in pair_perf.items():

                total = (
                    data["w"]
                    +
                    data["l"]
                )

                if total == 0:
                    continue

                wr = (
                    data["w"]
                    /
                    total
                ) * 100

                core = (
                    " [CORE]"
                    if symbol in CORE_PAIRS
                    else ""
                )

                line = (
                    f"• {symbol}{core}: "
                    f"{data['w']}W/"
                    f"{data['l']}L "
                    f"(WR: {wr:.1f}%)\n"
                )

                if (
                    total
                    <
                    CONFIG["MIN_TRADES"]
                ):

                    waiting += line

                elif (
                    symbol in CORE_PAIRS
                    or
                    wr >= CONFIG["MIN_WR"]
                ):

                    active += line

                else:

                    blacklist += line

            send_telegram(
                "🏆 <b>PERFORMA KOIN</b>\n\n"
                "🔥 <b>AKTIF</b>\n"
                f"{active or 'Belum ada'}\n"
                "⏳ <b>EVALUASI</b>\n"
                f"{waiting or 'Belum ada'}\n"
                "🗑️ <b>BLACKLIST</b>\n"
                f"{blacklist or 'Belum ada'}"
            )

            return

        # =========================================================
        # PAUSE
        # =========================================================

        elif command == "/pause":

            IS_BOT_PAUSED = True

            send_telegram(
                "⏸️ <b>BOT PAUSED</b>"
            )

            return

        # =========================================================
        # RESUME
        # =========================================================

        elif command == "/resume":

            IS_BOT_PAUSED = False

            send_telegram(
                "▶️ <b>BOT RESUMED</b>"
            )

            return

        # =========================================================
        # CANCEL ALL
        # =========================================================

        elif command == "/cancelall":

            cancel_all_pending_orders_on_pause()

            return

        # =========================================================
        # STAT
        # =========================================================

        elif command == "/stat":

            conn = sqlite3.connect(
                DB_FILE,
                timeout=5
            )

            stats = conn.execute(
                """
                SELECT
                    status,
                    COUNT(*)
                FROM trades
                GROUP BY status
                ORDER BY status
                """
            ).fetchall()

            conn.close()

            if stats:

                text_stats = "\n".join(
                    [
                        f"• {status}: {count}"
                        for status, count
                        in stats
                    ]
                )

            else:

                text_stats = (
                    "Belum ada data."
                )

            send_telegram(
                "📈 <b>STATISTIK</b>\n\n"
                f"{text_stats}"
            )

            return

        # =========================================================
        # UNKNOWN
        # =========================================================

        elif command.startswith("/"):

            send_telegram(
                "❓ Command tidak dikenal.\n"
                "Gunakan /help"
            )

    except Exception as e:

        print(
            f"[COMMAND ERROR] {e}",
            flush=True
        )

        try:

            send_telegram(
                "⚠️ <b>COMMAND ERROR</b>\n"
                f"<code>{e}</code>"
            )

        except Exception:
            pass


# =================================================================
# TELEGRAM POLLING
# =================================================================

def telegram_polling():

    print(
        "[TELEGRAM] Polling started...",
        flush=True
    )

    last_id = 0

    while True:

        try:

            url = (
                f"https://api.telegram.org/"
                f"bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            )

            response = requests.get(
                url,
                params={
                    "offset": last_id + 1,
                    "timeout": 5
                },
                timeout=10
            )

            data = response.json()

            if not data.get("ok"):

                print(
                    f"[TELEGRAM API ERROR] "
                    f"{data}",
                    flush=True
                )

                time.sleep(3)

                continue

            for update in data.get(
                "result",
                []
            ):

                last_id = update[
                    "update_id"
                ]

                # -------------------------------------------------
                # IMPORTANT
                # Command diproses di thread terpisah.
                # Jadi /posisi tidak memblokir /settings.
                # -------------------------------------------------

                threading.Thread(
                    target=process_telegram_update,
                    args=(update,),
                    daemon=True
                ).start()

        except Exception as e:

            print(
                f"[TELEGRAM POLLING ERROR] "
                f"{e}",
                flush=True
            )

            time.sleep(3)

        time.sleep(0.2)


# =================================================================
# WEB DASHBOARD
# =================================================================

app = Flask(
    __name__,
    template_folder="templates"
)


@app.route("/")
def dashboard():

    return render_template(
        "index.html"
    )


@app.route("/api/stats")
def api_stats():

    try:

        conn = sqlite3.connect(
            DB_FILE,
            timeout=10
        )

        df = pd.read_sql_query(
            """
            SELECT
                symbol,
                side,
                status,
                created_at
            FROM trades
            WHERE status IN
            ('CLOSED (TP)', 'CLOSED (SL)')
            ORDER BY id ASC
            """,
            conn
        )

        conn.close()

        if df.empty:

            return jsonify({
                "dates": [],
                "pnl": [],
                "win_rate": 0,
                "wins": 0,
                "losses": 0,
                "net_pnl": 0,
                "recent": []
            })

        margin = CONFIG.get(
            "MARGIN_PER_TRADE",
            2.0
        )

        df["pnl_value"] = df[
            "status"
        ].apply(
            lambda x:
            margin
            if "TP" in x
            else -margin
        )

        df["cum_pnl"] = (
            df["pnl_value"]
            .cumsum()
        )

        df["date_only"] = (
            pd.to_datetime(
                df["created_at"]
            )
            .dt.strftime(
                "%d %b %H:%M"
            )
        )

        wins = len(
            df[
                df["status"]
                ==
                "CLOSED (TP)"
            ]
        )

        losses = len(
            df[
                df["status"]
                ==
                "CLOSED (SL)"
            ]
        )

        total = wins + losses

        win_rate = (
            round(
                (wins / total) * 100,
                1
            )
            if total > 0
            else 0
        )

        net_pnl = round(
            df[
                "cum_pnl"
            ].iloc[-1],
            2
        )

        recent = (
            df.tail(10)
            [::-1]
            .to_dict("records")
        )

        return jsonify({
            "dates":
                df["date_only"].tolist(),

            "pnl":
                df["cum_pnl"].tolist(),

            "win_rate":
                win_rate,

            "wins":
                wins,

            "losses":
                losses,

            "net_pnl":
                net_pnl,

            "recent":
                recent
        })

    except Exception as e:

        print(
            f"[DASHBOARD ERROR] {e}",
            flush=True
        )

        return jsonify({
            "error": str(e)
        }), 500


# =================================================================
# MAIN LOOP
# =================================================================

def main_loop():

    last_hb = time.time()

    send_telegram(
        "🚀 <b>ALCHEMIST v2 INITIALIZED</b>"
    )

    while True:

        try:

            now = datetime.now(WIB)

            # -----------------------------------------------------
            # New day reset - WIB
            # -----------------------------------------------------

            if (
                now.hour == 7
                and now.minute == 0
                and now.second < 15
            ):

                global daily_sl_count
                global IS_BOT_PAUSED

                daily_sl_count = 0

                if IS_BOT_PAUSED:

                    IS_BOT_PAUSED = False

                    send_telegram(
                        "🌅 <b>NEW DAY RESET</b>\n"
                        "Bot kembali RUNNING."
                    )

            # -----------------------------------------------------
            # Heartbeat
            # -----------------------------------------------------

            if (
                time.time() - last_hb
                >= 21600
            ):

                send_telegram(
                    "💓 <b>PING</b>\n"
                    "Sistem normal."
                )

                last_hb = time.time()

            # -----------------------------------------------------
            # Daily SL
            # -----------------------------------------------------

            check_and_update_daily_sl()

            # -----------------------------------------------------
            # Update orders
            # -----------------------------------------------------

            update_orders_and_positions()

            # -----------------------------------------------------
            # Scanner
            # -----------------------------------------------------

            if (
                not IS_BOT_PAUSED
                and
                is_kill_zone()
            ):

                scan_markets()

        except Exception as e:

            print(
                f"[MAIN LOOP ERROR] {e}",
                flush=True
            )

            send_error_alert(
                str(e)
            )

        time.sleep(15)


# =================================================================
# START APPLICATION
# =================================================================

if __name__ == "__main__":

    print(
        "========================================",
        flush=True
    )

    print(
        "     ALCHEMIST v2 STARTING",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    # -------------------------------------------------------------
    # Database
    # -------------------------------------------------------------

    init_db()

    # -------------------------------------------------------------
    # Settings
    # -------------------------------------------------------------

    load_settings()

    # -------------------------------------------------------------
    # CEX
    # -------------------------------------------------------------

    reload_exchanges()

    print(
        f"[CONFIG] DRY RUN: "
        f"{CONFIG['DRY_RUN']}",
        flush=True
    )

    print(
        f"[CONFIG] MARGIN: "
        f"${CONFIG['MARGIN_PER_TRADE']}",
        flush=True
    )

    print(
        f"[CONFIG] MAX POS: "
        f"{CONFIG['MAX_CONCURRENT_POSITIONS']}",
        flush=True
    )

    print(
        f"[CONFIG] MIN WR: "
        f"{CONFIG['MIN_WR']}%",
        flush=True
    )

    # -------------------------------------------------------------
    # Telegram
    # -------------------------------------------------------------

    threading.Thread(
        target=telegram_polling,
        daemon=True
    ).start()

    # -------------------------------------------------------------
    # Flask dashboard
    # -------------------------------------------------------------

    threading.Thread(
        target=lambda:
        app.run(
            host="0.0.0.0",
            port=5000,
            threaded=True
        ),
        daemon=True
    ).start()

    # -------------------------------------------------------------
    # Main bot
    # -------------------------------------------------------------

    main_loop()
