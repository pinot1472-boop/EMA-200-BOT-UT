import asyncio
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
import ccxt.async_support as ccxt


def env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "si", "on")


# ======================= CONFIG (variables de entorno) =======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "3m,5m").split(",") if t.strip()]
EMA_PERIOD = int(os.getenv("EMA_PERIOD", "200"))
CANDLES = int(os.getenv("CANDLES", "500"))
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "0"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
CLOSE_DELAY_SEC = int(os.getenv("CLOSE_DELAY_SEC", "3"))
MARKET_TYPE = os.getenv("MARKET_TYPE", "swap")  # swap o spot

# --- Solo cripto: excluye acciones, indices, materias primas (petroleo...), forex y tokens de acciones ---
EXCLUDE_NON_CRYPTO = env_bool("EXCLUDE_NON_CRYPTO", True)
_DEFAULT_EXCLUDE = ("PAXG,XAUT,AAPLX,TSLAX,NVDAX,MSFTX,GOOGLX,AMZNX,METAX,COINX,MSTRX,HOODX,CRCLX,"
                    "SPYX,QQQX,NFLXX,AVGOX,JPMX,LLYX,WMTX,XOMX,ORCLX,AMDX,PLTRX,INTCX,IBMX,MCDX,GMEX")
EXCLUDE_BASES = {x.strip().upper() for x in os.getenv("EXCLUDE_BASES", _DEFAULT_EXCLUDE).split(",") if x.strip()}
# BingX nombra los contratos TradFi con prefijo NCCO (materias primas), NCSK (acciones),
# NCSI (indices) y NCFX (forex), p.ej. NCSKAAPL2USD, NCCOZINC2USD
NON_CRYPTO_RE = re.compile(r"^NC(CO|SK|SI|FX)")
EXCLUDED = []  # se rellena al cargar mercados

# --- Modulo 1: toque a la EMA200 ---
ENABLE_EMA_TOUCH = env_bool("ENABLE_EMA_TOUCH", True)
TOUCH_TOLERANCE_PCT = float(os.getenv("TOUCH_TOLERANCE_PCT", "0.0"))

# --- Modulo 2: UT Bot Alerts + filtro EMA200 ---
ENABLE_UT_BOT = env_bool("ENABLE_UT_BOT", True)
UT_KEY_VALUE = float(os.getenv("UT_KEY_VALUE", "1"))
UT_ATR_PERIOD = int(os.getenv("UT_ATR_PERIOD", "10"))
UT_HEIKIN_ASHI = env_bool("UT_HEIKIN_ASHI", False)
UT_STRICT_ABOVE = env_bool("UT_STRICT_ABOVE", False)

# --- Modulo 3: Radar de candidatas a explotar (arriba / abajo) ---
ENABLE_RADAR = env_bool("ENABLE_RADAR", True)
RADAR_TOP_N = int(os.getenv("RADAR_TOP_N", "5"))                    # candidatas por lado y por escaneo
RADAR_MIN_SCORE = float(os.getenv("RADAR_MIN_SCORE", "55"))         # puntuacion minima (0-100)
RADAR_MIN_VOTES = int(os.getenv("RADAR_MIN_VOTES", "3"))            # votos minimos de direccion (de 5)
RADAR_COOLDOWN_SCANS = int(os.getenv("RADAR_COOLDOWN_SCANS", "3"))  # no repetir la misma moneda/lado
RADAR_EVAL_SCANS = int(os.getenv("RADAR_EVAL_SCANS", "6"))          # velas para evaluar si acerto
RADAR_LEN = int(os.getenv("RADAR_LEN", "20"))                       # periodo BB/KC/momentum
RADAR_LOOKBACK = int(os.getenv("RADAR_LOOKBACK", "100"))            # velas para percentil de anchura
RADAR_BW_PCT_MAX = float(os.getenv("RADAR_BW_PCT_MAX", "15"))       # "estrecha": percentil <= 15
RADAR_MIN_ATR_PCT = float(os.getenv("RADAR_MIN_ATR_PCT", "0.03"))   # descarta monedas muertas
RADAR_USE_FUNDING = env_bool("RADAR_USE_FUNDING", False)            # voto contrarista por funding extremo
RADAR_FUNDING_EXTREME = float(os.getenv("RADAR_FUNDING_EXTREME", "0.05"))  # en %

# --- Resumen diario ---
REPORT_TZ = os.getenv("REPORT_TZ", "Europe/Madrid")
REPORT_TIME = os.getenv("REPORT_TIME", "23:59")
FEE_PCT_ROUNDTRIP = float(os.getenv("FEE_PCT_ROUNDTRIP", "0.1"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ema-bot")

# ======================= ESTADO EN MEMORIA =======================
POSITIONS = {}          # (symbol, tf) -> {"side", "entry"}
CLOSED_TRADES = []      # operaciones UT cerradas desde el ultimo resumen
FUNDING = {}            # symbol -> funding rate en %
RADAR_SCAN_COUNT = {}   # tf -> nº de escaneos
RADAR_LAST = {}         # (symbol, tf, dir) -> nº de escaneo del ultimo aviso
RADAR_PENDING = []      # picks pendientes de evaluar
RADAR_RESULTS = []      # picks ya evaluados (para el resumen diario)


# ======================= UTILIDADES =======================
def tf_seconds(tf: str) -> int:
    unit, n = tf[-1], int(tf[:-1])
    return n * {"m": 60, "h": 3600, "d": 86400}[unit]


def calc_ema(values, period):
    """EMA con semilla SMA. Lista alineada con values (None al inicio)."""
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema = [None] * (period - 1)
    prev = sum(values[:period]) / period
    ema.append(prev)
    for v in values[period:]:
        prev = v * k + prev * (1 - k)
        ema.append(prev)
    return ema


def touches(candle, ema):
    tol = TOUCH_TOLERANCE_PCT / 100
    return candle[3] <= ema * (1 + tol) and candle[2] >= ema * (1 - tol)


def fmt_link(symbol):
    base = symbol.split("/")[0]
    if MARKET_TYPE == "swap":
        return f"https://bingx.com/en/perpetual/{base}-USDT/"
    return f"https://bingx.com/en/spot/{base}USDT/"


def is_non_crypto(market):
    base = (market.get("base") or "").upper()
    mid = (market.get("id") or "").upper()
    return bool(NON_CRYPTO_RE.match(base) or NON_CRYPTO_RE.match(mid) or base in EXCLUDE_BASES)


# ======================= UT BOT (port del Pine v4) =======================
def ut_bot(candles):
    """Devuelve (buy, sell, trailing_stop) evaluado en la ULTIMA vela."""
    n = len(candles)
    c = UT_ATR_PERIOD
    if n < c + 5:
        return None
    opens = [x[1] for x in candles]
    highs = [x[2] for x in candles]
    lows = [x[3] for x in candles]
    closes = [x[4] for x in candles]

    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = [None] * n
    atr[c - 1] = sum(tr[:c]) / c
    for i in range(c, n):
        atr[i] = (atr[i - 1] * (c - 1) + tr[i]) / c

    if UT_HEIKIN_ASHI:
        src = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4 for i in range(n)]
    else:
        src = closes

    prev_ts = 0.0
    buy = sell = False
    cur = 0.0
    for i in range(c - 1, n):
        nloss = UT_KEY_VALUE * atr[i]
        s, s1 = src[i], src[i - 1]
        if s > prev_ts and s1 > prev_ts:
            cur = max(prev_ts, s - nloss)
        elif s < prev_ts and s1 < prev_ts:
            cur = min(prev_ts, s + nloss)
        elif s > prev_ts:
            cur = s - nloss
        else:
            cur = s + nloss
        if i == n - 1:
            above = s > cur and s1 <= prev_ts
            below = cur > s and prev_ts <= s1
            buy = s > cur and above
            sell = s < cur and below
        prev_ts = cur
    return buy, sell, cur


# ======================= RADAR: candidatas a explotar =======================
def linreg_last(vals):
    n = len(vals)
    mx = (n - 1) / 2
    my = sum(vals) / n
    sxx = sum((x - mx) ** 2 for x in range(n))
    sxy = sum((x - mx) * (v - my) for x, v in enumerate(vals))
    b = sxy / sxx if sxx else 0.0
    return my - b * mx + b * (n - 1)


def ttm_momentum(highs, lows, closes, end, L):
    """Histograma de momentum del TTM Squeeze (regresion lineal) en la vela 'end'."""
    vals = []
    for j in range(end - L + 1, end + 1):
        hh = max(highs[j - L + 1:j + 1])
        ll = min(lows[j - L + 1:j + 1])
        sm = sum(closes[j - L + 1:j + 1]) / L
        vals.append(closes[j] - ((hh + ll) / 2 + sm) / 2)
    return linreg_last(vals)


def radar_metrics(closed, ema200, funding=None):
    """
    Puntua (0-100) lo cerca que esta una moneda de una expansion de volatilidad y
    estima la direccion por votos. La compresion predice CUANDO/CUANTO, no la direccion;
    la direccion se estima con 5 votos (momentum, aceleracion, EMA200, media 20 y ruptura).
    """
    L, look = RADAR_LEN, RADAR_LOOKBACK
    n = len(closed)
    if n < look + L + 2:
        return None
    highs = [c[2] for c in closed]
    lows = [c[3] for c in closed]
    closes = [c[4] for c in closed]
    vols = [c[5] or 0 for c in closed]

    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))

    sq, bw = [], []
    for i in range(n - look, n):
        w = closes[i - L + 1:i + 1]
        sma = sum(w) / L
        sd = (sum((x - sma) ** 2 for x in w) / L) ** 0.5
        atr = sum(tr[i - L + 1:i + 1]) / L
        bw.append(4 * sd / sma if sma > 0 else 0.0)
        # BB(2sd) dentro de KC(1.5*ATR)  <=>  2*sd < 1.5*ATR  (condicion TTM Squeeze)
        sq.append(atr > 0 and 2 * sd < 1.5 * atr)

    close = closes[-1]
    atr_now = sum(tr[-L:]) / L
    sma_now = sum(closes[-L:]) / L
    if atr_now <= 0 or close <= 0:
        return None
    atr_pct = atr_now / close * 100
    if atr_pct < RADAR_MIN_ATR_PCT:
        return None

    pct = sum(1 for x in bw if x <= bw[-1]) / len(bw) * 100   # percentil de anchura (bajo = estrecho)
    squeeze_now = sq[-1]
    fired = (not sq[-1]) and sq[-2]

    def run_ending(idx):
        r = 0
        while idx >= 0 and sq[idx]:
            r += 1
            idx -= 1
        return r

    if squeeze_now:
        state, run = "COMPRIMIDA", run_ending(len(sq) - 1)
    elif fired:
        state, run = "DISPARADA", run_ending(len(sq) - 2)
    elif pct <= RADAR_BW_PCT_MAX:
        state, run = "ESTRECHA", 0
    else:
        return None

    mom = ttm_momentum(highs, lows, closes, n - 1, L)
    mom_prev = ttm_momentum(highs, lows, closes, n - 2, L)
    avg_vol = sum(vols[-L - 1:-1]) / L
    rv = vols[-1] / avg_vol if avg_vol > 0 else 0.0
    brk_up = close > max(highs[-L - 1:-1])
    brk_dn = close < min(lows[-L - 1:-1])
    dist = (close - ema200) / ema200 * 100

    votes = 0
    votes += 1 if mom > 0 else (-1 if mom < 0 else 0)
    votes += 1 if mom > mom_prev else (-1 if mom < mom_prev else 0)
    votes += 1 if close > ema200 else -1
    votes += 1 if close > sma_now else (-1 if close < sma_now else 0)
    votes += 1 if brk_up else (-1 if brk_dn else 0)
    if funding is not None and abs(funding) >= RADAR_FUNDING_EXTREME:
        votes += -1 if funding > 0 else 1   # posicionamiento saturado -> sesgo contrario

    ad = abs(dist)
    prox = 10 if ad <= 0.5 else (6 if ad <= 1.5 else (3 if ad <= 3 else 0))
    score = (30 * (1 - pct / 100)                # compresion de bandas
             + 15 * min(run, 15) / 15            # tiempo comprimida
             + (10 if state == "DISPARADA" else 0)  # liberacion confirmada
             + 20 * min(rv, 4) / 4               # volumen relativo
             + prox                              # cercania a la EMA200
             + 15 * min(abs(mom) / atr_now, 1))  # fuerza del momentum

    if votes >= RADAR_MIN_VOTES:
        direction = "UP"
    elif votes <= -RADAR_MIN_VOTES:
        direction = "DOWN"
    else:
        direction = "NEUTRAL"
    return {"score": score, "direction": direction, "votes": votes, "state": state, "run": run,
            "pct": pct, "rv": rv, "dist": dist, "atr_pct": atr_pct, "price": close}


def radar_line(rank, it):
    r = it["radar"]
    base = it["symbol"].split("/")[0]
    estado = {"DISPARADA": "💥 DISPARADA", "COMPRIMIDA": "⏳ COMPRIMIDA", "ESTRECHA": "↔️ ESTRECHA"}[r["state"]]
    run = f" ({r['run']}v)" if r["run"] else ""
    extra = ""
    if it.get("touch"):
        extra += " · 📍toque EMA"
    if it.get("ut"):
        extra += f" · UT {it['ut']}"
    return (f'{rank}. <a href="{fmt_link(it["symbol"])}"><b>{base}</b></a> <b>{r["score"]:.0f}</b> pts · '
            f'{estado}{run} · RVOL {r["rv"]:.1f}x · EMA{EMA_PERIOD} {r["dist"]:+.2f}% · '
            f'votos {r["votes"]:+d}{extra}')


async def radar_report(session, tf, infos, total):
    idx = RADAR_SCAN_COUNT.get(tf, 0) + 1
    RADAR_SCAN_COUNT[tf] = idx
    prices = {i["symbol"]: i["price"] for i in infos}

    # 1) evaluar picks anteriores de este timeframe
    pending = []
    for p in RADAR_PENDING:
        if p["tf"] != tf:
            pending.append(p)
        elif idx - p["idx"] >= RADAR_EVAL_SCANS:
            now = prices.get(p["symbol"])
            if now:
                ret = (now - p["price"]) / p["price"] * 100 * (1 if p["dir"] == "UP" else -1)
                RADAR_RESULTS.append({"tf": tf, "dir": p["dir"], "ret": ret,
                                      "atr_pct": p["atr_pct"], "state": p["state"]})
        else:
            pending.append(p)
    RADAR_PENDING[:] = pending

    # 2) seleccionar las mejores de cada lado
    cands = [i for i in infos if i["radar"] and i["radar"]["score"] >= RADAR_MIN_SCORE
             and i["radar"]["direction"] != "NEUTRAL"]

    def pick(direction):
        lst = [i for i in cands if i["radar"]["direction"] == direction]
        lst.sort(key=lambda i: (-i["radar"]["score"], -abs(i["radar"]["votes"])))
        out = []
        for i in lst:
            if idx - RADAR_LAST.get((i["symbol"], tf, direction), -10 ** 9) < RADAR_COOLDOWN_SCANS:
                continue
            out.append(i)
            if len(out) >= RADAR_TOP_N:
                break
        return out

    ups, downs = pick("UP"), pick("DOWN")
    if not ups and not downs:
        return
    for direction, lst in (("UP", ups), ("DOWN", downs)):
        for i in lst:
            RADAR_LAST[(i["symbol"], tf, direction)] = idx
            RADAR_PENDING.append({"symbol": i["symbol"], "tf": tf, "dir": direction, "idx": idx,
                                  "price": i["radar"]["price"], "atr_pct": i["radar"]["atr_pct"],
                                  "state": i["radar"]["state"]})
    hora = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"🎯 <b>Radar {tf}</b> · {hora} · {total} monedas"]
    if ups:
        lines += ["", "🟢 <b>Candidatas ALCISTAS</b>"] + [radar_line(k + 1, i) for k, i in enumerate(ups)]
    if downs:
        lines += ["", "🔴 <b>Candidatas BAJISTAS</b>"] + [radar_line(k + 1, i) for k, i in enumerate(downs)]
    await send_telegram(session, "\n".join(lines))


def radar_summary():
    res = RADAR_RESULTS
    if not res:
        return []
    out = ["", f"<b>Calidad del radar</b> (a {RADAR_EVAL_SCANS} velas, sin comisiones)"]

    def stats(rr):
        hit = sum(1 for r in rr if r["ret"] > 0) / len(rr) * 100
        big = sum(1 for r in rr if r["ret"] >= r["atr_pct"]) / len(rr) * 100
        avg = sum(r["ret"] for r in rr) / len(rr)
        return f"{len(rr)} picks · {hit:.0f}% a favor · {big:.0f}% ≥1 ATR · media {avg:+.2f}%"

    for d, label in (("UP", "Alcistas"), ("DOWN", "Bajistas")):
        rr = [r for r in res if r["dir"] == d]
        if rr:
            out.append(f"{label}: {stats(rr)}")
    for st, label in (("DISPARADA", "💥 Disparadas"), ("COMPRIMIDA", "⏳ Comprimidas"), ("ESTRECHA", "↔️ Estrechas")):
        rr = [r for r in res if r["state"] == st]
        if rr:
            out.append(f"{label}: {stats(rr)}")
    return out


# ======================= TELEGRAM =======================
async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Falta TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.error("Telegram %s: %s", r.status, await r.text())
    except Exception as e:
        log.error("Error Telegram: %s", e)


async def send_long(session, text):
    """Divide mensajes largos (limite Telegram 4096 caracteres)."""
    chunk = ""
    for line in text.split("\n"):
        if len(chunk) + len(line) + 1 > 3800:
            await send_telegram(session, chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        await send_telegram(session, chunk)


# ======================= MERCADOS =======================
async def get_symbols(ex):
    await ex.load_markets(True)
    symbols, excluded = [], []
    for s, m in ex.markets.items():
        if not m.get("active", True) or m.get("quote") != "USDT":
            continue
        if MARKET_TYPE == "swap" and not (m.get("swap") and m.get("linear")):
            continue
        if MARKET_TYPE == "spot" and not m.get("spot"):
            continue
        if EXCLUDE_NON_CRYPTO and is_non_crypto(m):
            excluded.append(m.get("base") or s)
            continue
        symbols.append(s)
    EXCLUDED[:] = sorted(excluded)
    if EXCLUDED:
        log.info("Excluidos no-cripto: %d (%s...)", len(EXCLUDED), ", ".join(EXCLUDED[:8]))
    if MIN_VOLUME_USDT > 0:
        try:
            tickers = await ex.fetch_tickers(symbols)
            symbols = [s for s in symbols
                       if (tickers.get(s, {}).get("quoteVolume") or 0) >= MIN_VOLUME_USDT]
        except Exception as e:
            log.warning("No se pudo filtrar por volumen: %s", e)
    return symbols


async def refresh_funding(ex):
    if not (ENABLE_RADAR and RADAR_USE_FUNDING and MARKET_TYPE == "swap"):
        return
    try:
        fr = await ex.fetch_funding_rates()
        FUNDING.clear()
        for s, v in fr.items():
            r = (v or {}).get("fundingRate")
            if r is not None:
                FUNDING[s] = float(r) * 100
        log.info("Funding cargado: %d monedas", len(FUNDING))
    except Exception as e:
        log.warning("No se pudo cargar funding: %s", e)


# ======================= ANALISIS POR MONEDA =======================
async def analyze(ex, session, sem, symbol, tf):
    async with sem:
        try:
            ohlcv = await ex.fetch_ohlcv(symbol, tf, limit=CANDLES)
        except Exception as e:
            log.debug("%s %s error: %s", symbol, tf, e)
            return 0, None
    now_ms = int(time.time() * 1000)
    tf_ms = tf_seconds(tf) * 1000
    closed = [c for c in ohlcv if c[0] + tf_ms <= now_ms]
    if len(closed) < EMA_PERIOD + 2:
        return 0, None
    ema = calc_ema([c[4] for c in closed], EMA_PERIOD)
    last, prev = closed[-1], closed[-2]
    e_last, e_prev = ema[-1], ema[-2]
    if e_last is None or e_prev is None:
        return 0, None

    hora = datetime.fromtimestamp(last[0] / 1000, tz=timezone.utc).strftime("%H:%M UTC")
    link = f'<a href="{fmt_link(symbol)}">Abrir en BingX</a>'
    sent = 0
    info = {"symbol": symbol, "price": last[4], "touch": False, "ut": None, "radar": None}

    # ---------- Modulo 1: toque a la EMA200 (toque nuevo) ----------
    if ENABLE_EMA_TOUCH and touches(last, e_last) and not touches(prev, e_prev):
        origen = ("desde ARRIBA 🔻 (posible soporte)" if prev[4] > e_prev
                  else "desde ABAJO 🔺 (posible resistencia)")
        cierre = "arriba" if last[4] > e_last else "abajo"
        await send_telegram(session,
            f"🚨 <b>Toque EMA{EMA_PERIOD}</b> · {tf}\n<b>{symbol}</b>\n"
            f"Toque {origen}\n"
            f"EMA{EMA_PERIOD}: <code>{e_last:.8g}</code>\n"
            f"Cierre: <code>{last[4]:.8g}</code> ({cierre} de la EMA)\n"
            f"Vela: {hora}\n{link}")
        sent += 1
        info["touch"] = True

    # ---------- Modulo 2: UT Bot + filtro EMA200 ----------
    if ENABLE_UT_BOT:
        ut = ut_bot(closed)
        if ut:
            buy, sell, stop = ut
            key = (symbol, tf)
            pos = POSITIONS.get(key)
            price = last[4]

            if pos and ((pos["side"] == "long" and sell) or (pos["side"] == "short" and buy)):
                if pos["side"] == "long":
                    pnl = (price - pos["entry"]) / pos["entry"] * 100
                else:
                    pnl = (pos["entry"] - price) / pos["entry"] * 100
                pnl -= FEE_PCT_ROUNDTRIP
                lado = "LONG" if pos["side"] == "long" else "SHORT"
                icono = "✅ GANADA" if pnl > 0 else "❌ PERDIDA"
                await send_telegram(session,
                    f"⚠️ <b>SALIR {lado} AHORA</b> · {tf}\n<b>{symbol}</b>\n"
                    f"Señal contraria UT Bot ({'Sell' if sell else 'Buy'})\n"
                    f"Entrada: <code>{pos['entry']:.8g}</code>\n"
                    f"Salida: <code>{price:.8g}</code>\n"
                    f"{icono}: <b>{pnl:+.2f}%</b> (neto de comisiones)\n"
                    f"Vela: {hora}\n{link}")
                CLOSED_TRADES.append({"symbol": symbol, "tf": tf, "side": pos["side"],
                                      "entry": pos["entry"], "exit": price, "pnl": pnl})
                del POSITIONS[key]
                sent += 1

            above_ema = (last[3] > e_last) if UT_STRICT_ABOVE else (price > e_last)
            below_ema = (last[2] < e_last) if UT_STRICT_ABOVE else (price < e_last)
            if buy and above_ema:
                POSITIONS[key] = {"side": "long", "entry": price}
                await send_telegram(session,
                    f"🟢 <b>UT BOT · BUY (LONG)</b> · {tf}\n<b>{symbol}</b>\n"
                    f"Precio: <code>{price:.8g}</code>\n"
                    f"EMA{EMA_PERIOD}: <code>{e_last:.8g}</code> (precio por encima ✅)\n"
                    f"Trailing stop UT: <code>{stop:.8g}</code>\n"
                    f"Salida: cuando aparezca Sell\n"
                    f"Vela: {hora}\n{link}")
                sent += 1
                info["ut"] = "Buy"
            elif sell and below_ema:
                POSITIONS[key] = {"side": "short", "entry": price}
                await send_telegram(session,
                    f"🔴 <b>UT BOT · SELL (SHORT)</b> · {tf}\n<b>{symbol}</b>\n"
                    f"Precio: <code>{price:.8g}</code>\n"
                    f"EMA{EMA_PERIOD}: <code>{e_last:.8g}</code> (precio por debajo ✅)\n"
                    f"Trailing stop UT: <code>{stop:.8g}</code>\n"
                    f"Salida: cuando aparezca Buy\n"
                    f"Vela: {hora}\n{link}")
                sent += 1
                info["ut"] = "Sell"

    # ---------- Modulo 3: metricas del radar (se envian agrupadas en scan) ----------
    if ENABLE_RADAR:
        info["radar"] = radar_metrics(closed, e_last, FUNDING.get(symbol) if RADAR_USE_FUNDING else None)
    return sent, info


async def scan(ex, session, tf, symbols):
    sem = asyncio.Semaphore(CONCURRENCY)
    t0 = time.time()
    res = await asyncio.gather(*[analyze(ex, session, sem, s, tf) for s in symbols])
    sent = sum(r[0] for r in res)
    infos = [r[1] for r in res if r[1]]
    if ENABLE_RADAR:
        try:
            await radar_report(session, tf, infos, len(symbols))
        except Exception as e:
            log.error("Error radar %s: %s", tf, e)
    log.info("[%s] %d monedas, %d mensajes, %d posiciones virtuales, %.1fs",
             tf, len(symbols), sent, len(POSITIONS), time.time() - t0)


# ======================= RESUMEN DIARIO =======================
def short_name(symbol):
    return symbol.split(":")[0]


def trade_line(t):
    icon = "✅" if t["pnl"] > 0 else "❌"
    return f"{icon} {short_name(t['symbol'])} {t['side'].upper()} {t['tf']} <b>{t['pnl']:+.2f}%</b>"


def build_report(trades, date_str):
    open_n = len(POSITIONS)
    if not trades:
        lines = [f"📊 <b>Resumen del día {date_str}</b>",
                 "Sin operaciones UT cerradas hoy.", f"Posiciones abiertas: {open_n}"]
        return "\n".join(lines + radar_summary())
    wins = sorted([t for t in trades if t["pnl"] > 0], key=lambda t: -t["pnl"])
    losses = sorted([t for t in trades if t["pnl"] <= 0], key=lambda t: t["pnl"])
    n = len(trades)
    total = sum(t["pnl"] for t in trades)
    avg_w = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
    avg_l = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
    lines = [
        f"📊 <b>Resumen del día {date_str}</b> (neto de comisiones)",
        f"Operaciones cerradas: <b>{n}</b>",
        f"✅ Ganadoras: <b>{len(wins)}</b> ({len(wins) / n * 100:.0f}%) · media {avg_w:+.2f}%",
        f"❌ Perdedoras: <b>{len(losses)}</b> ({len(losses) / n * 100:.0f}%) · media {avg_l:+.2f}%",
        f"Resultado total (suma de %): <b>{total:+.2f}%</b>",
        f"Posiciones aún abiertas: {open_n}",
        "",
        "<b>Por timeframe</b>",
    ]
    for tf in sorted({t["tf"] for t in trades}, key=tf_seconds):
        tt = [t for t in trades if t["tf"] == tf]
        w = sum(1 for t in tt if t["pnl"] > 0)
        lines.append(f"{tf}: {len(tt)} ops · {w}G/{len(tt) - w}P · {sum(t['pnl'] for t in tt):+.2f}%")
    lines += ["", "<b>Mejores</b>"] + [trade_line(t) for t in wins[:10]]
    lines += ["", "<b>Peores</b>"] + [trade_line(t) for t in losses[:10]]
    lines += radar_summary()
    return "\n".join(lines)


async def daily_report_loop(session):
    try:
        tz = ZoneInfo(REPORT_TZ)
    except Exception:
        log.warning("Zona horaria invalida (%s), uso UTC", REPORT_TZ)
        tz = timezone.utc
    hh, mm = (int(x) for x in REPORT_TIME.split(":"))
    while True:
        now = datetime.now(tz)
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        trades = CLOSED_TRADES[:]
        CLOSED_TRADES.clear()
        try:
            await send_long(session, build_report(trades, target.strftime("%d/%m/%Y")))
        except Exception as e:
            log.error("Error resumen diario: %s", e)
        RADAR_RESULTS.clear()


# ======================= BUCLES =======================
async def tf_loop(ex, session, tf, symbols_ref):
    secs = tf_seconds(tf)
    while True:
        now = time.time()
        nxt = (now // secs + 1) * secs + CLOSE_DELAY_SEC
        await asyncio.sleep(nxt - now)
        try:
            await scan(ex, session, tf, symbols_ref["list"])
        except Exception as e:
            log.error("Error en scan %s: %s", tf, e)


async def refresh_symbols(ex, symbols_ref):
    while True:
        await asyncio.sleep(3600)
        try:
            symbols_ref["list"] = await get_symbols(ex)
            log.info("Lista actualizada: %d monedas", len(symbols_ref["list"]))
            await refresh_funding(ex)
        except Exception as e:
            log.error("Error actualizando lista: %s", e)


async def main():
    ex = ccxt.bingx({"enableRateLimit": True,
                     "options": {"defaultType": "swap" if MARKET_TYPE == "swap" else "spot"}})
    async with aiohttp.ClientSession() as session:
        try:
            symbols = await get_symbols(ex)
            log.info("Monedas BingX (%s): %d", MARKET_TYPE, len(symbols))
            await refresh_funding(ex)
            symbols_ref = {"list": symbols}
            modulos = []
            if ENABLE_EMA_TOUCH:
                modulos.append(f"Toque EMA{EMA_PERIOD}")
            if ENABLE_UT_BOT:
                modulos.append(f"UT Bot (key={UT_KEY_VALUE}, ATR={UT_ATR_PERIOD}) + filtro EMA{EMA_PERIOD}")
            if ENABLE_RADAR:
                modulos.append(f"Radar top {RADAR_TOP_N} (min {RADAR_MIN_SCORE:.0f} pts)")
            excl = (f"\nExcluidas no-cripto: {len(EXCLUDED)} "
                    f"(ej. {', '.join(EXCLUDED[:5])})") if EXCLUDED else ""
            await send_telegram(session,
                f"✅ Bot iniciado\nTF: {', '.join(TIMEFRAMES)}\n"
                f"Módulos: {' | '.join(modulos)}\nResumen diario: {REPORT_TIME} ({REPORT_TZ})\n"
                f"Monedas cripto: {len(symbols)} ({MARKET_TYPE}){excl}\n\n"
                f"Radar: 💥 DISPARADA = squeeze liberado en la última vela · "
                f"⏳ COMPRIMIDA = en squeeze · ↔️ ESTRECHA = bandas muy estrechas")
            await asyncio.gather(refresh_symbols(ex, symbols_ref),
                                 daily_report_loop(session),
                                 *[tf_loop(ex, session, tf, symbols_ref) for tf in TIMEFRAMES])
        finally:
            await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
