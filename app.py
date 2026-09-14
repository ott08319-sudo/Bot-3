import os, json, hmac, hashlib, time, uuid, threading, re, random, secrets
from decimal import Decimal
from functools import wraps
from math import comb

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from flask import Flask, request, jsonify, render_template

app = Flask(__name__, template_folder=".", static_folder=".")

# ─────────── ENV ───────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ["DATABASE_URL"]
BC_KEY       = os.environ.get("BYTECOIN_API_KEY", "")
BC_SECRET    = os.environ.get("BYTECOIN_WEBHOOK_SECRET", "")
BC_ENABLED   = os.environ.get("BYTECOIN_ENABLED", "false").lower() == "true"
ADMIN_TOKEN  = os.environ.get("ADMIN_TOKEN", "")
SELF_URL     = os.environ.get("SELF_URL", "")
BC_BASE      = "https://bytecoin.space/api/public/v1"
MIN_TRANSFER = Decimal("0.0000001")
HOUSE_EDGE   = Decimal("0.02")

# ─────────── DB ───────────
def conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    with conn() as c, c.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id BIGINT PRIMARY KEY,
            username TEXT,
            balance NUMERIC(20,9) DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS bets (
            id BIGSERIAL PRIMARY KEY,
            tg_id BIGINT, game TEXT,
            bet NUMERIC(20,9), payout NUMERIC(20,9),
            meta JSONB, created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id BIGSERIAL PRIMARY KEY,
            tg_id BIGINT, direction TEXT,
            amount NUMERIC(20,9), idem_key TEXT UNIQUE,
            transaction_id TEXT, replayed BOOLEAN,
            status TEXT DEFAULT 'pending', raw JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_id
          ON transfers(transaction_id) WHERE transaction_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS webhook_inbox (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID UNIQUE NOT NULL,
            event_type TEXT, payload JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS webhook_jobs (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID UNIQUE REFERENCES webhook_inbox(event_id),
            event_type TEXT, payload JSONB,
            status TEXT DEFAULT 'pending',
            retry_count INT DEFAULT 0, last_error TEXT,
            processed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_pending
          ON webhook_jobs(id) WHERE status='pending';
        """)

def get_or_create_user(tg_id, username=""):
    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO users (tg_id, username) VALUES (%s,%s)
                       ON CONFLICT (tg_id) DO UPDATE SET username=EXCLUDED.username
                       RETURNING *""", (tg_id, username))
        return cur.fetchone()

def get_balance(tg_id):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT balance FROM users WHERE tg_id=%s", (tg_id,))
        r = cur.fetchone()
        return r["balance"] if r else Decimal(0)

def adjust_balance(tg_id, delta):
    with conn() as c, c.cursor() as cur:
        cur.execute("""UPDATE users SET balance=balance+%s
                       WHERE tg_id=%s RETURNING balance""", (delta, tg_id))
        return cur.fetchone()["balance"]

def record_bet(tg_id, game, bet, payout, meta):
    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO bets (tg_id, game, bet, payout, meta)
                       VALUES (%s,%s,%s,%s,%s)""",
                    (tg_id, game, bet, payout, json.dumps(meta)))

# ─────────── AUTH ───────────
def validate_init_data(init_data):
    try:
        from urllib.parse import parse_qsl
        parsed = dict(parse_qsl(init_data, strict_parsing=True))
    except Exception:
        return None
    h = parsed.pop("hash", None)
    if not h:
        return None
    check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, h):
        return None
    try:
        return json.loads(parsed.get("user", "{}"))
    except Exception:
        return None

def require_auth(f):
    @wraps(f)
    def w(*a, **kw):
        u = validate_init_data(request.headers.get("X-Init-Data", ""))
        if not u:
            return jsonify({"error": "unauthorized"}), 401
        request.tg_user = u
        return f(*a, **kw)
    return w

# ─────────── BYTECOIN API ───────────
def _bc_headers(idem=None):
    h = {"Content-Type": "application/json"}
    if BC_KEY.startswith("Bearer "):
        h["Authorization"] = BC_KEY
    else:
        h["X-API-Key"] = BC_KEY
    if idem:
        h["Idempotency-Key"] = idem
    return h

def _bc_request(method, path, idem=None, **kw):
    if not BC_ENABLED:
        return {"status": "error", "code": "DISABLED", "error": "Bytecoin disabled"}
    for attempt in range(3):
        try:
            r = requests.request(method, BC_BASE + path,
                                 headers=_bc_headers(idem),
                                 timeout=10, **kw)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "1")))
                continue
            return r.json()
        except requests.RequestException as e:
            if attempt == 2:
                return {"status": "error", "code": "NETWORK", "error": str(e)}
            time.sleep(2 ** attempt)

_rate_cache = {"v": None, "t": 0}

def exchange_rate():
    now = time.time()
    if _rate_cache["v"] and now - _rate_cache["t"] < 60:
        return _rate_cache["v"]
    try:
        r = requests.get(f"{BC_BASE}/commerce/exchangeRate", timeout=10)
        d = r.json().get("data", {})
        if not d.get("available"):
            return None
        rates = d["rates"]
        out = {
            "reference": Decimal(d["reference_rate"]),
            "sell": Decimal(rates["selling"]["Bytecoin"]["RUB"]["1"]),
            "buy":  Decimal(rates["buying"]["Bytecoin"]["RUB"]["1"]),
            "updated_at": d["updated_at"],
        }
        _rate_cache.update(v=out, t=now)
        return out
    except Exception:
        return None

def bc_users_info(user_ids):
    if not user_ids:
        return {}
    ids = [int(x) for x in dict.fromkeys(user_ids)][:100]
    r = _bc_request("POST", "/users/info", json={"user_ids": ids})
    if r.get("status") != "ok":
        return {}
    return {str(i["id"]): i for i in r.get("data", {}).get("items", [])}

def bc_transfer(user_id, amount, idem):
    amount = Decimal(amount).quantize(Decimal("0.000000001"))
    if amount < MIN_TRANSFER:
        return {"status": "error", "code": "VALIDATION_ERROR",
                "error": f"min {MIN_TRANSFER}"}
    return _bc_request("POST", "/service/transfer", idem=idem,
                       json={"user_id": int(user_id), "sum": f"{amount:.9f}"})

def bc_transfer_ok(r): return r.get("status") == "ok"
def bc_stat():         return _bc_request("GET", "/service/stat")
def bc_maintenance(on):return _bc_request("POST", "/service/maintenance", json={"on": bool(on)})

# ─────────── GAMES ───────────
def g_dice(bet, target, over):
    roll = random.randint(1, 100)
    win = (roll > target) if over else (roll < target)
    chance = (100 - target) if over else (target - 1)
    if chance <= 0:
        return {"win": False, "roll": roll, "payout": Decimal(0)}
    mult = (Decimal(100) * (1 - HOUSE_EDGE) / Decimal(chance)).quantize(Decimal("0.0001"))
    payout = (bet * mult).quantize(Decimal("0.000000001")) if win else Decimal(0)
    return {"win": win, "roll": roll, "multiplier": float(mult), "payout": payout}

def g_coin(bet, side):
    r = secrets.choice(["heads", "tails"])
    win = r == side
    return {"win": win, "result": r,
            "payout": (bet * Decimal("1.96")).quantize(Decimal("0.000000001")) if win else Decimal(0)}

def g_roulette(bet, btype, value=None):
    n = random.randint(0, 36)
    red = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
    win, mult = False, Decimal(0)
    if btype == "red" and n in red:      win, mult = True, Decimal("2")
    elif btype == "black" and n not in red and n != 0: win, mult = True, Decimal("2")
    elif btype == "even" and n != 0 and n % 2 == 0:    win, mult = True, Decimal("2")
    elif btype == "odd" and n % 2 == 1:  win, mult = True, Decimal("2")
    elif btype == "number" and int(value) == n:        win, mult = True, Decimal("36")
    payout = (bet * mult * (1 - HOUSE_EDGE)).quantize(Decimal("0.000000001")) if win else Decimal(0)
    return {"win": win, "number": n, "payout": payout}

SYM = ["🍒","🍋","🔔","💎","7️⃣","⭐"]
PAY = {"🍒":2, "🍋":3, "🔔":5, "💎":10, "7️⃣":20, "⭐":50}

def g_slots(bet):
    r = [random.choice(SYM) for _ in range(3)]
    if r[0] == r[1] == r[2]:
        mult = Decimal(PAY[r[0]])
        return {"win": True, "reels": r, "multiplier": float(mult),
                "payout": (bet * mult * (1 - HOUSE_EDGE)).quantize(Decimal("0.000000001"))}
    if len(set(r)) == 2:
        return {"win": True, "reels": r, "multiplier": 1.5,
                "payout": (bet * Decimal("1.5")).quantize(Decimal("0.000000001"))}
    return {"win": False, "reels": r, "payout": Decimal(0)}

def g_crash_point():
    if random.random() < 0.01:
        return Decimal("1.00")
    return (Decimal(100) / (Decimal(100) - Decimal(random.randint(1, 99)))).quantize(Decimal("0.01"))

def g_mines(bet, mines_count, picked):
    mine_cells = set(secrets.SystemRandom().sample(range(25), mines_count))
    if any(c in mine_cells for c in picked):
        return {"win": False, "mine_cells": sorted(mine_cells), "payout": Decimal(0)}
    p = len(picked)
    if p == 0:
        return {"win": True, "multiplier": 1.0, "payout": bet, "mine_cells": sorted(mine_cells)}
    mult = (Decimal(comb(25, p)) / Decimal(comb(25 - mines_count, p)) * (1 - HOUSE_EDGE)).quantize(Decimal("0.0001"))
    return {"win": True, "multiplier": float(mult),
            "payout": (bet * mult).quantize(Decimal("0.000000001")),
            "mine_cells": sorted(mine_cells)}

# ─────────── WEBHOOK ───────────
def verify_webhook(headers, raw):
    event_id  = headers.get("x-bytecoin-event-id", "")
    ts        = headers.get("x-bytecoin-timestamp", "")
    sig       = headers.get("x-bytecoin-signature", "")
    if not BC_SECRET or not event_id or not re.fullmatch(r"\d{10}", ts):
        raise ValueError("invalid")
    if abs(time.time() - int(ts)) > 300:
        raise ValueError("expired")
    signed = f"{ts}.{event_id}.".encode() + raw
    expected = "v1=" + hmac.new(BC_SECRET.encode(), signed, hashlib.sha256).hexdigest()
    if not re.fullmatch(r"v1=[0-9a-f]{64}", sig) or not hmac.compare_digest(sig, expected):
        raise ValueError("bad sig")
    ev = json.loads(raw)
    if ev.get("id") != event_id:
        raise ValueError("id mismatch")
    return ev

def save_event_and_job(event):
    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO webhook_inbox (event_id, event_type, payload)
                       VALUES (%s,%s,%s) ON CONFLICT (event_id) DO NOTHING
                       RETURNING id""",
                    (event["id"], event.get("event"), json.dumps(event)))
        if not cur.fetchone():
            return False
        cur.execute("""INSERT INTO webhook_jobs (event_id, event_type, payload)
                       VALUES (%s,%s,%s)""",
                    (event["id"], event.get("event"), json.dumps(event)))
    return True

@app.route("/webhooks/bytecoin", methods=["POST"])
def webhook():
    raw = request.get_data()
    h = {k.lower(): v for k, v in request.headers.items()}
    try:
        event = verify_webhook(h, raw)
    except Exception as e:
        print(f"[webhook] verify failed: {e}", flush=True)
        return jsonify({"code": "INVALID_WEBHOOK_SIGNATURE"}), 401
    print(f"[webhook] received event_type={event.get('event')} id={event.get('id')}", flush=True)
    try:
        save_event_and_job(event)
    except Exception as e:
        print(f"[webhook] storage: {e}", flush=True)
        return jsonify({"code": "WEBHOOK_STORAGE_UNAVAILABLE"}), 503
    return "", 204

# ─────────── WORKER ───────────
def process_jobs():
    while True:
        try:
            with conn() as c, c.cursor() as cur:
                cur.execute("""SELECT id, event_type, payload FROM webhook_jobs
                               WHERE status='pending' ORDER BY id LIMIT 20
                               FOR UPDATE SKIP LOCKED""")
                jobs = cur.fetchall()
                for j in jobs:
                    try:
                        if j["event_type"] == "transfer.received":
                            apply_deposit(j["payload"])
                        cur.execute("""UPDATE webhook_jobs SET status='done',
                                       processed_at=now() WHERE id=%s""", (j["id"],))
                    except Exception as e:
                        print(f"[worker] job {j['id']} failed: {e}", flush=True)
                        cur.execute("""UPDATE webhook_jobs
                                       SET retry_count=retry_count+1, last_error=%s,
                                       status=CASE WHEN retry_count>=10 THEN 'failed'
                                                   ELSE 'pending' END
                                       WHERE id=%s""", (str(e), j["id"]))
        except Exception as e:
            print(f"[worker] {e}", flush=True)
        time.sleep(3)

def apply_deposit(event):
    print(f"[deposit] event={json.dumps(event)}", flush=True)
    d = event.get("data", {})
    side = d.get("side")
    print(f"[deposit] side={side} user_id={d.get('user_id')} sum={d.get('sum')} tx={d.get('transaction_id')}", flush=True)

    if side not in ("to_service", "from_user"):
        print(f"[deposit] SKIP side={side}", flush=True)
        return

    tx_id = d.get("transaction_id")
    if not tx_id:
        print("[deposit] SKIP no transaction_id", flush=True)
        return
    try:
        user_id = int(d["user_id"])
        amount = Decimal(str(d["sum"])).quantize(Decimal("0.000000001"))
    except Exception as e:
        print(f"[deposit] SKIP parse error: {e}", flush=True)
        return

    if amount <= 0:
        print(f"[deposit] SKIP amount={amount}", flush=True)
        return

    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO transfers
                       (tg_id, direction, amount, idem_key, status, transaction_id, raw)
                       VALUES (%s,'in',%s,%s,'ok',%s,%s)
                       ON CONFLICT (transaction_id) DO NOTHING
                       RETURNING id""",
                    (user_id, amount, f"wh-{tx_id}", tx_id, json.dumps(event)))
        if not cur.fetchone():
            print(f"[deposit] SKIP duplicate tx={tx_id}", flush=True)
            return
        cur.execute("""INSERT INTO users (tg_id, balance) VALUES (%s,%s)
                       ON CONFLICT (tg_id) DO UPDATE
                       SET balance = users.balance + EXCLUDED.balance""",
                    (user_id, amount))
        print(f"[deposit] OK user={user_id} amount={amount}", flush=True)

# ─────────── ROUTES ───────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/health")
def health():
    return {"ok": True}

@app.route("/api/me")
@require_auth
def me():
    u = request.tg_user
    get_or_create_user(u["id"], u.get("username", ""))
    bal = get_balance(u["id"])
    rate = exchange_rate()
    return {
        "balance": str(bal),
        "balance_rub": str((bal * rate["sell"]).quantize(Decimal("0.01"))) if rate else None,
        "rate": {"sell": str(rate["sell"]), "buy": str(rate["buy"])} if rate else None,
    }

@app.route("/api/bet", methods=["POST"])
@require_auth
def bet():
    u = request.tg_user
    d = request.json or {}
    game = d.get("game")
    amount = Decimal(str(d.get("amount", "0"))).quantize(Decimal("0.000000001"))
    if amount <= 0 or get_balance(u["id"]) < amount:
        return {"error": "insufficient_funds"}, 400

    adjust_balance(u["id"], -amount)

    if   game == "dice":     res = g_dice(amount, int(d["target"]), bool(d["over"]))
    elif game == "coinflip": res = g_coin(amount, d["side"])
    elif game == "roulette": res = g_roulette(amount, d["bet_type"], d.get("value"))
    elif game == "slots":    res = g_slots(amount)
    elif game == "crash":    res = {"win": True, "point": str(g_crash_point()), "payout": amount}
    elif game == "mines":    res = g_mines(amount, int(d["mines"]), d["picked"])
    else:
        adjust_balance(u["id"], amount)
        return {"error": "unknown_game"}, 400

    payout = res.get("payout", Decimal(0))
    if payout > 0:
        adjust_balance(u["id"], payout)
    record_bet(u["id"], game, amount, payout, {k: str(v) for k, v in res.items()})

    return {
        "result": {k: str(v) for k, v in res.items()},
        "balance": str(get_balance(u["id"])),
    }

@app.route("/api/withdraw", methods=["POST"])
@require_auth
def withdraw():
    u = request.tg_user
    amount = Decimal(str(request.json.get("amount", "0"))).quantize(Decimal("0.000000001"))

    if amount < MIN_TRANSFER:
        return {"error": "min_withdraw", "min": str(MIN_TRANSFER)}, 400
    if get_balance(u["id"]) < amount:
        return {"error": "insufficient_funds"}, 400

    if BC_ENABLED and not bc_users_info([u["id"]]).get(str(u["id"])):
        return {"error": "receiver_not_registered"}, 400

    idem = f"wd-{u['id']}-{uuid.uuid4()}"

    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO transfers (tg_id, direction, amount, idem_key, status)
                       VALUES (%s,'out',%s,%s,'pending') RETURNING id""",
                    (u["id"], amount, idem))
        row_id = cur.fetchone()["id"]

    adjust_balance(u["id"], -amount)
    resp = bc_transfer(u["id"], amount, idem)

    with conn() as c, c.cursor() as cur:
        if bc_transfer_ok(resp):
            cur.execute("""UPDATE transfers SET status='ok', transaction_id=%s,
                           raw=%s, replayed=%s WHERE id=%s""",
                        (resp.get("transaction_id"), json.dumps(resp),
                         resp.get("replayed", False), row_id))
            return {"status": "ok", "transaction_id": resp.get("transaction_id")}
        cur.execute("UPDATE transfers SET status='failed', raw=%s WHERE id=%s",
                    (json.dumps(resp), row_id))
        adjust_balance(u["id"], amount)
        return {"error": resp.get("code", "FAILED"), "message": resp.get("error", "")}, 400

@app.route("/admin/stats")
def admin_stats():
    if request.headers.get("X-Admin-Token") != ADMIN_TOKEN or not ADMIN_TOKEN:
        return {"error": "forbidden"}, 403
    stat = bc_stat()
    if stat.get("status") != "ok":
        return {"error": "api_down", "detail": stat}, 502
    d = stat["data"]
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(balance),0) AS t FROM users")
        users_total = cur.fetchone()["t"]
    sb = Decimal(d["balance"]); hold = Decimal(d["in_hold"])
    liability = users_total + hold
    cov = float(sb / liability) if liability > 0 else None
    return {
        "service_balance": str(sb), "users_total": str(users_total),
        "in_hold": str(hold), "coverage_ratio": cov,
        "warning": "LOW_COVERAGE" if cov and cov < 1.1 else None,
        "today": {"in": d["transactions_in"], "out": d["transactions_out"],
                  "coins_in": d["coins_in"], "coins_out": d["coins_out"]},
    }

@app.route("/admin/freeze", methods=["POST"])
def admin_freeze():
    if request.headers.get("X-Admin-Token") != ADMIN_TOKEN or not ADMIN_TOKEN:
        return {"error": "forbidden"}, 403
    return bc_maintenance(bool(request.json.get("on")))

# ─────────── BOOT ───────────
_worker_started = False
def start_worker_once():
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
    init_db()
    threading.Thread(target=process_jobs, daemon=True).start()
    print("[worker] started", flush=True)

start_worker_once()

def keep_alive():
    if not SELF_URL:
        return
    while True:
        time.sleep(600)
        try: requests.get(SELF_URL + "/health", timeout=10)
        except Exception: pass

threading.Thread(target=keep_alive, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
