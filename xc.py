#!/usr/bin/env python3
"""x_cluster: X API v2 (pay-per-use) collector -> local SQLite, with pseudonymized export.

Usage (Windows):  py xc.py <command> [--yes]
Commands:
  check      Verify the token with one cheap count request (~$0.005)
  run        Do everything (count, search, users, probe, phase2, export) in one go
  count      Count matching posts per query (1 request per query, ~$0.005 each)
  search     Collect seed posts for each query in config.json
  users      Sample authors from seed posts and fetch their profiles
  probe      Phase 1: count each sampled account's baseline / everyday / matching posts
  phase2     Phase 2: pick a stratified subsample and fetch its pre-controversy posts
  timelines  (optional, not in run) Fetch recent posts of the sampled accounts
  export     Build analysis.sqlite (pseudonymized) for analysis
  sync       Re-check stored posts/users against X and delete removed ones (costs money; needs --yes)
  purge      Delete raw.sqlite and analysis.sqlite (needs --yes)
  status     Show what is stored and the estimated spend so far

Only the Python standard library is used (Python 3.9+).
"""
import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import random
import re
import secrets
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
API = "https://api.x.com/2"
RAW_DB = os.path.join(BASE, "raw.sqlite")
OUT_DB = os.path.join(BASE, "analysis.sqlite")
SALT_FILE = os.path.join(BASE, "salt.secret")
CONFIG = os.path.join(BASE, "config.json")

TWEET_FIELDS = ("created_at,lang,public_metrics,referenced_tweets,entities,"
                "conversation_id,in_reply_to_user_id,author_id,edit_history_tweet_ids")
USER_FIELDS = ("created_at,description,public_metrics,protected,verified,verified_type,"
               "url,location,pinned_tweet_id,profile_image_url")

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


class BudgetExceeded(Exception):
    pass


# ---------------------------------------------------------------- setup

def load_env():
    path = os.path.join(BASE, ".env")
    env = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    tok = env.get("X_BEARER_TOKEN") or os.environ.get("X_BEARER_TOKEN")
    if not tok:
        sys.exit(".env に X_BEARER_TOKEN が見つかりません。")
    return tok


def load_config():
    with open(CONFIG, encoding="utf-8-sig") as f:
        return json.load(f)


def raw_db():
    con = sqlite3.connect(RAW_DB)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS posts(
        id TEXT PRIMARY KEY, author_id TEXT, created_at TEXT, text TEXT, lang TEXT,
        conversation_id TEXT, in_reply_to_user_id TEXT, referenced TEXT, mentions TEXT,
        metrics TEXT, source TEXT, fetched_at TEXT);
    CREATE INDEX IF NOT EXISTS posts_author ON posts(author_id);
    CREATE TABLE IF NOT EXISTS seed_hits(post_id TEXT, query_name TEXT, PRIMARY KEY(post_id, query_name));
    CREATE TABLE IF NOT EXISTS search_state(query_name TEXT PRIMARY KEY, next_token TEXT, n INTEGER, done INTEGER);
    CREATE TABLE IF NOT EXISTS sample(user_id TEXT PRIMARY KEY, selected_at TEXT);
    CREATE TABLE IF NOT EXISTS users(
        id TEXT PRIMARY KEY, username TEXT, name TEXT, description TEXT, created_at TEXT,
        followers INTEGER, following INTEGER, tweet_count INTEGER, listed INTEGER,
        protected INTEGER, verified INTEGER, verified_type TEXT, url TEXT, location TEXT,
        pinned_tweet_id TEXT, profile_image_url TEXT, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS timeline_done(user_id TEXT PRIMARY KEY, n INTEGER, status TEXT, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS ledger(ts TEXT, endpoint TEXT, kind TEXT, units INTEGER, usd REAL);
    CREATE TABLE IF NOT EXISTS daily_counts(query_name TEXT, start TEXT, end TEXT, n INTEGER,
        PRIMARY KEY(query_name, start));
    CREATE TABLE IF NOT EXISTS search_day_state(query_name TEXT, start TEXT, quota INTEGER, n INTEGER,
        next_token TEXT, done INTEGER, PRIMARY KEY(query_name, start));
    CREATE TABLE IF NOT EXISTS probe(user_id TEXT PRIMARY KEY, baseline_total INTEGER, baseline_daily INTEGER,
        match_n INTEGER, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS phase2(user_id TEXT PRIMARY KEY, pred_class TEXT, selected_at TEXT, status TEXT, n INTEGER);
    """)
    cols = [r[1] for r in con.execute("PRAGMA table_info(sample)")]
    if "query_name" not in cols:
        con.execute("ALTER TABLE sample ADD COLUMN query_name TEXT")
    return con


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- API + budget

class Client:
    def __init__(self, token, con, cfg):
        self.token, self.con, self.cfg = token, con, cfg
        self.prices = cfg.get("prices", {})
        self.budget = float(cfg.get("budget_usd", 0))

    def spent(self):
        return self.con.execute("SELECT COALESCE(SUM(usd),0) FROM ledger").fetchone()[0]

    def guard(self, projected):
        if self.spent() + projected > self.budget:
            raise BudgetExceeded(
                f"予算上限 ${self.budget:.2f} に達するため停止しました（推定使用額 ${self.spent():.2f}）。"
                " config.json の budget_usd を見直してください。")

    def charge(self, endpoint, kind, units):
        usd = units * float(self.prices.get(kind, 0))
        self.con.execute("INSERT INTO ledger VALUES(?,?,?,?,?)", (now(), endpoint, kind, units, usd))
        self.con.commit()

    def get(self, path, params, projected):
        self.guard(projected)
        url = f"{API}{path}?{urllib.parse.urlencode(params)}"
        for attempt in range(6):
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {self.token}", "User-Agent": "x_cluster/1.0"})
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace")[:800]
                if e.code == 429:
                    reset = e.headers.get("x-rate-limit-reset")
                    wait = max(5, int(reset) - int(time.time()) + 2) if reset else 60
                    print(f"  レート制限: {wait}秒待機します…")
                    time.sleep(min(wait, 960))
                    continue
                if e.code >= 500:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise SystemExit(f"HTTP {e.code} {path}\n{body}")
            except urllib.error.URLError as e:
                print(f"  通信エラー（{e.reason}）。再試行します…")
                time.sleep(5 * (attempt + 1))
        raise SystemExit(f"再試行しても失敗しました: {path}")


# ---------------------------------------------------------------- storage helpers

def store_post(con, t, source):
    ents = t.get("entities") or {}
    mentions = [{"id": m.get("id"), "username": m.get("username")} for m in ents.get("mentions", [])]
    con.execute("""INSERT INTO posts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET text=excluded.text, metrics=excluded.metrics,
        referenced=excluded.referenced, mentions=excluded.mentions, fetched_at=excluded.fetched_at,
        source=CASE WHEN posts.source LIKE '%'||excluded.source||'%' THEN posts.source
                    ELSE posts.source||','||excluded.source END""",
        (t["id"], t.get("author_id"), t.get("created_at"), t.get("text"), t.get("lang"),
         t.get("conversation_id"), t.get("in_reply_to_user_id"),
         json.dumps(t.get("referenced_tweets") or []), json.dumps(mentions),
         json.dumps(t.get("public_metrics") or {}), source, now()))


def store_user(con, u):
    pm = u.get("public_metrics") or {}
    con.execute("""INSERT OR REPLACE INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (u["id"], u.get("username"), u.get("name"), u.get("description"), u.get("created_at"),
         pm.get("followers_count"), pm.get("following_count"), pm.get("tweet_count"),
         pm.get("listed_count"), int(bool(u.get("protected"))), int(bool(u.get("verified"))),
         u.get("verified_type"), u.get("url"), u.get("location"), u.get("pinned_tweet_id"),
         u.get("profile_image_url"), now()))


def time_params(cfg):
    p = {}
    if cfg.get("start_time"):
        p["start_time"] = cfg["start_time"]
    if cfg.get("end_time"):
        p["end_time"] = cfg["end_time"]
    return p


# ---------------------------------------------------------------- commands

def seed_cap(cfg, q):
    return int(q.get("max_seed_posts", cfg.get("max_seed_posts_per_query", 500)))


def n_accounts(cfg, q):
    if "n_accounts" in q:
        return int(q["n_accounts"])
    k = int(cfg.get("max_accounts", 200))
    return -(-k // max(1, len(cfg["queries"])))  # ceil


def estimate(cl, cfg):
    """Max cost: seeds + phase 1 (profile + 3 counts per account) + phase 2 (baseline posts)."""
    p = cl.prices
    per_p1 = p.get("user", 0) + 3 * p.get("count_all_request", 0)
    total = 0.0
    for q in cfg["queries"]:
        buckets = cl.con.execute("SELECT start, end, n FROM daily_counts WHERE query_name=? ORDER BY start",
                                 (q["name"],)).fetchall()
        if buckets:  # each day's request returns at least 10 posts, so small quotas cost a bit extra
            seeds = sum(max(x, 10) for x in allocate(seed_cap(cfg, q), buckets) if x > 0)
        else:
            seeds = seed_cap(cfg, q)
        total += seeds * p.get("post", 0) + n_accounts(cfg, q) * per_p1
    total += int(cfg.get("phase2_accounts", 160)) * int(cfg.get("phase2_posts_per_account", 30)) * p.get("post", 0)
    return total


def cmd_run(cl, cfg):
    """count -> search -> users (phase 1 sample) -> probe -> phase2 -> export, in one go (ADR-0011)."""
    have = {r[0] for r in cl.con.execute("SELECT DISTINCT query_name FROM daily_counts")}
    if any(q["name"] not in have for q in cfg["queries"]):
        cmd_count(cl, cfg)
    print(f"\n最大見積もり ${estimate(cl, cfg):.2f}（予算上限 ${cl.budget:.2f}、ここまでの推定使用額 ${cl.spent():.2f}）")
    print("予算上限に達した時点で自動停止します。Ctrl+C で中断しても、再実行すれば続きから再開します。")
    print("件数確認APIには回数制限があるため、待ち時間を含めて数時間かかることがあります。PCはつけたままにしてください。\n")
    cmd_search(cl, cfg)
    cmd_users(cl, cfg)
    cmd_probe(cl, cfg)
    cmd_phase2(cl, cfg)
    cmd_export(cl.con, cfg)
    print("\n完了しました。Claude に「終わった」と伝えてください。")


def cmd_check(cl, cfg):
    d = cl.get("/tweets/counts/recent", {"query": "の lang:ja -is:retweet", "granularity": "day"}, 0.01)
    cl.charge("counts/recent", "count_request", 1)
    print("トークンは有効です。直近7日間で「の」を含む日本語投稿数（RT除く）:",
          f"{d.get('meta', {}).get('total_tweet_count', '?'):,}")


def count_query(cl, cfg, query, start=None, end=None):
    """Return list of (start, end, n) day buckets for a query over the configured period (or start/end)."""
    ep = "all" if cfg.get("search_endpoint") == "all" else "recent"
    window = {"start_time": start, "end_time": end} if start else time_params(cfg)
    params = {"query": query, "granularity": "day", **{k: v for k, v in window.items() if v}}
    out, pages = [], 0
    while True:
        d = cl.get(f"/tweets/counts/{ep}", params, 0.02)
        cl.charge(f"counts/{ep}", "count_request" if ep == "recent" else "count_all_request", 1)
        out += [(b["start"], b["end"], int(b.get("tweet_count", 0))) for b in d.get("data", [])]
        pages += 1
        nt = d.get("meta", {}).get("next_token")
        if not nt or pages >= 20:
            break
        params["next_token"] = nt
        if ep == "all":
            time.sleep(1.1)
    return out


def cmd_count(cl, cfg):
    ep = "all" if cfg.get("search_endpoint") == "all" else "recent"
    total = 0
    for q in cfg["queries"]:
        buckets = count_query(cl, cfg, q["query"])
        cl.con.execute("DELETE FROM daily_counts WHERE query_name=?", (q["name"],))
        cl.con.executemany("INSERT OR REPLACE INTO daily_counts VALUES(?,?,?,?)",
                           [(q["name"], s, e, n) for s, e, n in buckets])
        cl.con.commit()
        n = sum(b[2] for b in buckets)
        total += n
        cap = seed_cap(cfg, q)
        peak = max(buckets, key=lambda b: b[2]) if buckets else None
        print(f"[{q['name']}] 該当 {n:,} 件 → 収集予定 {min(n, cap):,} 件（約 ${min(n, cap) * cl.prices.get('post', 0):.2f}）"
              + (f"  ピーク {peak[0][:10]}（{peak[2]:,} 件）" if peak else ""))
    print(f"合計 {total:,} 件（対象期間: {'全期間検索' if ep == 'all' else '直近7日'}"
          f"{'・設定期間' if time_params(cfg) else ''}）")


def allocate(cap, buckets):
    """Largest-remainder allocation of cap across day buckets, proportional to their counts."""
    total = sum(n for _, _, n in buckets)
    if total <= cap:
        return [n for _, _, n in buckets]
    raw = [cap * n / total for _, _, n in buckets]
    q = [int(x) for x in raw]
    for i in sorted(range(len(raw)), key=lambda i: raw[i] - q[i], reverse=True)[:cap - sum(q)]:
        q[i] += 1
    return q


def cmd_search(cl, cfg):
    """Seed collection, stratified by day in proportion to daily counts (ADR-0009)."""
    con = cl.con
    ep = "all" if cfg.get("search_endpoint") == "all" else "recent"
    price = cl.prices.get("post", 0)
    for q in cfg["queries"]:
        buckets = con.execute("SELECT start, end, n FROM daily_counts WHERE query_name=? ORDER BY start",
                              (q["name"],)).fetchall()
        if not buckets:
            raise SystemExit(f"[{q['name']}] の日別件数がありません。先に py xc.py count を実行してください。")
        for (s, e, n), quota in zip(buckets, allocate(seed_cap(cfg, q), buckets)):
            con.execute("INSERT OR IGNORE INTO search_day_state VALUES(?,?,?,?,?,?)", (q["name"], s, quota, 0, None, 0))
        con.commit()
        todo = con.execute("SELECT start, quota, n, next_token FROM search_day_state "
                           "WHERE query_name=? AND done=0 AND quota>0 ORDER BY start", (q["name"],)).fetchall()
        ends = {s: e for s, e, _ in buckets}
        for s, quota, got, token in todo:
            while got < quota:
                params = {"query": q["query"], "max_results": max(10, min(100, quota - got)),
                          "tweet.fields": TWEET_FIELDS, "start_time": s, "end_time": ends[s]}
                if token:
                    params["next_token"] = token
                d = cl.get(f"/tweets/search/{ep}", params, 100 * price)
                data = d.get("data", [])
                cl.charge(f"search/{ep}", "post", len(data))
                for t in data[:quota - got]:
                    store_post(con, t, "seed")
                    con.execute("INSERT OR IGNORE INTO seed_hits VALUES(?,?)", (t["id"], q["name"]))
                got += min(len(data), quota - got)
                token = d.get("meta", {}).get("next_token")
                done = int(got >= quota or not token)
                con.execute("UPDATE search_day_state SET n=?, next_token=?, done=? WHERE query_name=? AND start=?",
                            (got, token, done, q["name"], s))
                con.commit()
                if ep == "all":
                    time.sleep(1.1)  # full-archive search: 1 request/sec
                if done:
                    break
        n_all = con.execute("SELECT COALESCE(SUM(n),0) FROM search_day_state WHERE query_name=?", (q["name"],)).fetchone()[0]
        print(f"[{q['name']}] {n_all:,} 件（日別に比例配分）  推定使用額 ${cl.spent():.2f}")
    print("検索完了。")


def cmd_users(cl, cfg):
    con = cl.con
    if not con.execute("SELECT 1 FROM sample LIMIT 1").fetchone():
        rng = random.Random(int(cfg.get("random_seed", 42)))
        chosen = set()
        for q in cfg["queries"]:
            pool = [r[0] for r in con.execute(
                "SELECT DISTINCT p.author_id FROM posts p JOIN seed_hits s ON s.post_id=p.id "
                "WHERE s.query_name=? ORDER BY p.author_id", (q["name"],))]
            free = [a for a in pool if a not in chosen]
            k = n_accounts(cfg, q)
            take = free if len(free) <= k else rng.sample(free, k)
            chosen.update(take)
            con.executemany("INSERT OR IGNORE INTO sample(user_id, selected_at, query_name) VALUES(?,?,?)",
                            [(a, now(), q["name"]) for a in take])
            print(f"  [{q['name']}] 投稿者 {len(pool):,} 人 → {len(take):,} 人を抽出")
        con.commit()
        print(f"合計 {len(chosen):,} 人を無作為抽出しました（検索条件ごとの層化抽出）。")
    todo = [r[0] for r in con.execute(
        "SELECT user_id FROM sample WHERE user_id NOT IN (SELECT id FROM users)")]
    price = cl.prices.get("user", 0)
    for i in range(0, len(todo), 100):
        batch = todo[i:i + 100]
        d = cl.get("/users", {"ids": ",".join(batch), "user.fields": USER_FIELDS}, len(batch) * price)
        data = d.get("data", [])
        cl.charge("users", "user", len(data))
        for u in data:
            store_user(con, u)
        con.commit()
        print(f"プロフィール {min(i + 100, len(todo)):,}/{len(todo):,}  推定使用額 ${cl.spent():.2f}")
    print("プロフィール取得完了。")


def cmd_timelines(cl, cfg):
    con = cl.con
    per = int(cfg.get("timeline_posts_per_account", 100))
    exclude = cfg.get("timeline_exclude", ["retweets"])
    price = cl.prices.get("post", 0)
    users = con.execute("""SELECT s.user_id, COALESCE(u.protected,0) FROM sample s
        LEFT JOIN users u ON u.id=s.user_id
        WHERE s.user_id NOT IN (SELECT user_id FROM timeline_done)""").fetchall()
    for idx, (uid, protected) in enumerate(users, 1):
        if protected:
            con.execute("INSERT OR REPLACE INTO timeline_done VALUES(?,?,?,?)", (uid, 0, "protected", now()))
            con.commit()
            continue
        n, token, status = 0, None, "ok"
        while n < per:
            params = {"max_results": min(100, max(5, per - n)), "tweet.fields": TWEET_FIELDS}
            if exclude:
                params["exclude"] = ",".join(exclude)
            if token:
                params["pagination_token"] = token
            d = cl.get(f"/users/{uid}/tweets", params, 100 * price)
            data = d.get("data", [])
            cl.charge("users/tweets", "post", len(data))
            for t in data:
                t.setdefault("author_id", uid)
                store_post(con, t, "timeline")
            n += len(data)
            if d.get("errors") and not data:
                status = "unavailable"
            token = d.get("meta", {}).get("next_token")
            if not token:
                break
        con.execute("INSERT OR REPLACE INTO timeline_done VALUES(?,?,?,?)", (uid, n, status, now()))
        con.commit()
        print(f"タイムライン {idx:,}/{len(users):,}（{n} 件）  推定使用額 ${cl.spent():.2f}")
    print("タイムライン取得完了。")


def cmd_probe(cl, cfg):
    """Phase 1 measurements per sampled account, using count requests only (ADR-0011):
    baseline_total = posts in the baseline window, baseline_daily = of those, posts with everyday words,
    match_n = posts matching its stratum query in the study window (for sampling weights, ADR-0009)."""
    con = cl.con
    qmap = {q["name"]: q["query"] for q in cfg["queries"]}
    bs, be = cfg["baseline_start"], cfg["baseline_end"]
    daily = "(" + " OR ".join(cfg["daily_words"]) + ")"
    todo = con.execute("""SELECT s.user_id, s.query_name, u.created_at FROM sample s JOIN users u ON u.id=s.user_id
        WHERE s.user_id NOT IN (SELECT user_id FROM probe) AND COALESCE(u.protected,0)=0""").fetchall()
    for i, (uid, qn, created) in enumerate(todo, 1):
        new = bool(created) and created >= be
        tot = 0 if new else sum(b[2] for b in count_query(cl, cfg, f"from:{uid} -is:retweet", bs, be))
        day = 0 if (new or tot == 0) else sum(b[2] for b in count_query(cl, cfg, f"from:{uid} {daily} -is:retweet", bs, be))
        m = sum(b[2] for b in count_query(cl, cfg, f"({qmap[qn]}) from:{uid}")) if qn in qmap else None
        con.execute("INSERT OR REPLACE INTO probe VALUES(?,?,?,?,?)", (uid, tot, day, m, now()))
        con.commit()
        if i % 25 == 0 or i == len(todo):
            print(f"第1段階の計測 {i:,}/{len(todo):,}  推定使用額 ${cl.spent():.2f}")


BIO_RE_CACHE = {}


def phase1_class(cfg, created, bio, tot, day):
    """Pre-registered phase-1 rule (ADR-0011). Returns specialized / general / low_activity."""
    r = cfg["phase1_rule"]
    pat = BIO_RE_CACHE.setdefault("p", re.compile("|".join(r["bio_patterns"])))
    if created and created >= r["new_account_after"]:
        return "specialized"
    if pat.search(bio or ""):
        return "specialized"
    if tot is None or tot < int(r["min_baseline_posts"]):
        return "low_activity"
    return "specialized" if (day or 0) / tot < float(r["daily_ratio_threshold"]) else "general"


def cmd_phase2(cl, cfg):
    """Select the phase-2 subsample (stratified by group x predicted class) and fetch baseline posts."""
    con = cl.con
    if not con.execute("SELECT 1 FROM phase2 LIMIT 1").fetchone():
        rng = random.Random(int(cfg.get("random_seed", 42)) + 1)
        groups = [q["name"] for q in cfg["queries"]]
        per_g = -(-int(cfg.get("phase2_accounts", 160)) // len(groups))
        rows = con.execute("""SELECT s.user_id, s.query_name, u.created_at, u.description, p.baseline_total, p.baseline_daily
            FROM sample s JOIN users u ON u.id=s.user_id JOIN probe p ON p.user_id=s.user_id""").fetchall()
        picked = []
        for g in groups:
            by_c = {}
            for uid, qn, cr, bio, tot, day in rows:
                if qn == g:
                    by_c.setdefault(phase1_class(cfg, cr, bio, tot, day), []).append(uid)
            for v in by_c.values():
                rng.shuffle(v)
            take, classes = [], sorted(by_c)
            while len(take) < per_g and any(by_c[c] for c in classes):  # round-robin across predicted classes
                for c in classes:
                    if by_c[c] and len(take) < per_g:
                        take.append((by_c[c].pop(), c))
            picked += take
            print(f"  [{g}] 第2段階 {len(take)} 人（" + "、".join(f"{c}:{sum(1 for _, x in take if x == c)}" for c in classes) + "）")
        con.executemany("INSERT INTO phase2 VALUES(?,?,?,?,?)", [(u, c, now(), None, None) for u, c in picked])
        con.commit()
    per = int(cfg.get("phase2_posts_per_account", 30))
    price = cl.prices.get("post", 0)
    todo = [r[0] for r in con.execute("SELECT user_id FROM phase2 WHERE status IS NULL")]
    for i, uid in enumerate(todo, 1):
        params = {"max_results": max(5, min(100, per)), "tweet.fields": TWEET_FIELDS, "exclude": "retweets",
                  "start_time": cfg["baseline_start"], "end_time": cfg["baseline_end"]}
        d = cl.get(f"/users/{uid}/tweets", params, per * price)
        data = d.get("data", [])[:per]
        cl.charge("users/tweets(baseline)", "post", len(d.get("data", [])))
        source, status = "baseline", "ok"
        if not data:  # no posts before the controversy (e.g. new account): judge from recent posts instead
            params.pop("start_time"), params.pop("end_time")
            d = cl.get(f"/users/{uid}/tweets", params, per * price)
            data = d.get("data", [])[:per]
            cl.charge("users/tweets(recent)", "post", len(d.get("data", [])))
            source, status = "recent", ("recent" if data else "empty")
        for t in data:
            t.setdefault("author_id", uid)
            store_post(con, t, source)
        con.execute("UPDATE phase2 SET status=?, n=? WHERE user_id=?", (status, len(data), uid))
        con.commit()
        if i % 20 == 0 or i == len(todo):
            print(f"第2段階の投稿取得 {i:,}/{len(todo):,}  推定使用額 ${cl.spent():.2f}")


# ---------------------------------------------------------------- pseudonymized export

def salt():
    if not os.path.exists(SALT_FILE):
        with open(SALT_FILE, "w", encoding="utf-8") as f:
            f.write(secrets.token_hex(32))
    with open(SALT_FILE, encoding="utf-8") as f:
        return bytes.fromhex(f.read().strip())


URL_RE = re.compile(r"https?://\S+")
AT_RE = re.compile(r"@[A-Za-z0-9_]{1,15}")


def cmd_export(con, cfg):
    s = salt()

    def pid(kind, x):
        if not x:
            return None
        return kind + hmac.new(s, f"{kind}:{x}".encode(), hashlib.sha256).hexdigest()[:16]

    def scrub_post(text, mentions):
        text = URL_RE.sub("<URL>", text or "")
        for m in sorted(mentions, key=lambda m: -len(m.get("username") or "")):
            if m.get("username") and m.get("id"):
                text = re.sub("@" + re.escape(m["username"]) + r"(?![A-Za-z0-9_])",
                              "@" + pid("u", m["id"]), text, flags=re.I)
        return re.sub(r"@(?!u[0-9a-f]{16})[A-Za-z0-9_]{1,15}", "@<USER>", text)

    def scrub_bio(text):
        return AT_RE.sub("@<USER>", URL_RE.sub("<URL>", text or ""))

    name_kw = cfg.get("name_keywords", [])
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)
    out = sqlite3.connect(OUT_DB)
    out.executescript("""
    CREATE TABLE accounts(pid TEXT PRIMARY KEY, created_at TEXT, followers INTEGER, following INTEGER,
        tweet_count INTEGER, listed INTEGER, protected INTEGER, verified INTEGER,
        bio TEXT, bio_mention_count INTEGER, bio_has_url INTEGER, has_url INTEGER, has_location INTEGER,
        default_image INTEGER, name_keyword_hits TEXT, seed_queries TEXT, seed_post_count INTEGER,
        timeline_status TEXT, timeline_n INTEGER, stratum TEXT, stratum_match_posts INTEGER,
        baseline_total INTEGER, baseline_daily INTEGER, pred_class TEXT, phase2 INTEGER, phase2_n INTEGER);
    CREATE TABLE daily_counts(query_name TEXT, day TEXT, n INTEGER);
    CREATE TABLE posts(pid TEXT PRIMARY KEY, author_pid TEXT, created_at TEXT, text TEXT, lang TEXT,
        conversation_pid TEXT, reply_to_user_pid TEXT, ref_type TEXT, ref_post_pid TEXT,
        mention_pids TEXT, likes INTEGER, reposts INTEGER, replies INTEGER, quotes INTEGER,
        is_seed INTEGER, seed_queries TEXT, in_sample INTEGER, source TEXT);
    CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
    """)
    sample = {r[0] for r in con.execute("SELECT user_id FROM sample")}
    seedq = {}
    for post_id, qn in con.execute("SELECT post_id, query_name FROM seed_hits"):
        seedq.setdefault(post_id, []).append(qn)
    acct_seed = {}
    for pid_, aid in con.execute("SELECT id, author_id FROM posts"):
        if pid_ in seedq:
            acct_seed.setdefault(aid, set()).update(seedq[pid_])
    acct_seed_n = dict(con.execute(
        "SELECT p.author_id, COUNT(DISTINCT p.id) FROM posts p JOIN seed_hits s ON s.post_id=p.id GROUP BY 1"))
    tl = {r[0]: (r[1], r[2]) for r in con.execute("SELECT user_id, n, status FROM timeline_done")}
    strat = dict(con.execute("SELECT user_id, query_name FROM sample"))
    probe = {r[0]: r[1:] for r in con.execute("SELECT user_id, baseline_total, baseline_daily, match_n FROM probe")}
    p2 = {r[0]: r[1] for r in con.execute("SELECT user_id, n FROM phase2")}

    for u in con.execute("SELECT * FROM users").fetchall():
        (uid, _uname, name, desc, created, fol, fng, tc, listed, prot, ver, _vt, url, loc,
         _pin, img, _f) = u
        hits = [k for k in name_kw if k and k in (name or "")]
        tstat = tl.get(uid, (None, None))
        pr = probe.get(uid, (None, None, None))
        pc = phase1_class(cfg, created, desc, pr[0], pr[1]) if uid in probe else None
        out.execute("INSERT INTO accounts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            pid("u", uid), created, fol, fng, tc, listed, prot, ver,
            scrub_bio(desc), len(AT_RE.findall(desc or "")), int(bool(URL_RE.search(desc or ""))),
            int(bool(url)), int(bool(loc)), int("default_profile" in (img or "")),
            json.dumps(hits, ensure_ascii=False), json.dumps(sorted(acct_seed.get(uid, [])), ensure_ascii=False),
            acct_seed_n.get(uid, 0), tstat[1], tstat[0], strat.get(uid), pr[2], pr[0], pr[1], pc,
            int(uid in p2), p2.get(uid)))

    for row in con.execute("SELECT id, author_id, created_at, text, lang, conversation_id, "
                           "in_reply_to_user_id, referenced, mentions, metrics, source FROM posts"):
        (i, aid, created, text, lang, conv, rto, refs, ments, mets, src_) = row
        refs, ments, mets = json.loads(refs or "[]"), json.loads(ments or "[]"), json.loads(mets or "{}")
        ref = refs[0] if refs else {}
        out.execute("INSERT INTO posts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            pid("p", i), pid("u", aid), created, scrub_post(text, ments), lang,
            pid("p", conv), pid("u", rto), ref.get("type"), pid("p", ref.get("id")),
            json.dumps([pid("u", m["id"]) for m in ments if m.get("id")]),
            mets.get("like_count"), mets.get("retweet_count"), mets.get("reply_count"), mets.get("quote_count"),
            int(i in seedq), json.dumps(sorted(seedq.get(i, [])), ensure_ascii=False), int(aid in sample), src_))

    out.executemany("INSERT INTO daily_counts VALUES(?,?,?)",
                    con.execute("SELECT query_name, substr(start,1,10), n FROM daily_counts").fetchall())
    out.executemany("INSERT INTO meta VALUES(?,?)", [
        ("exported_at", now()),
        ("queries", json.dumps([{"name": q["name"], "query": q["query"]} for q in cfg["queries"]], ensure_ascii=False)),
        ("search_endpoint", cfg.get("search_endpoint", "recent")),
        ("start_time", cfg.get("start_time") or ""), ("end_time", cfg.get("end_time") or ""),
        ("random_seed", str(cfg.get("random_seed", 42))),
        ("design", json.dumps({k: cfg.get(k) for k in ("baseline_start", "baseline_end", "daily_words", "phase1_rule",
                               "phase2_accounts", "phase2_posts_per_account")}, ensure_ascii=False)),
        ("n_accounts", json.dumps({q["name"]: n_accounts(cfg, q) for q in cfg["queries"]})),
        ("n_seed_authors", str(con.execute(
            "SELECT COUNT(DISTINCT p.author_id) FROM posts p JOIN seed_hits s ON s.post_id=p.id").fetchone()[0])),
    ])
    out.commit()
    a = out.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
    p = out.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    out.close()
    print(f"analysis.sqlite を作成しました（アカウント {a:,} 件・投稿 {p:,} 件、ID・@名・URLは仮名化済み）。")


# ---------------------------------------------------------------- compliance + housekeeping

def cmd_sync(cl, cfg, yes):
    con = cl.con
    ids = [r[0] for r in con.execute("SELECT id FROM posts")]
    uids = [r[0] for r in con.execute("SELECT id FROM users")]
    est = len(ids) * cl.prices.get("post", 0) + len(uids) * cl.prices.get("user", 0)
    print(f"保存中の投稿 {len(ids):,} 件・アカウント {len(uids):,} 件を X と照合します（推定 ${est:.2f}）。")
    if not yes:
        print("実行するには: py xc.py sync --yes")
        return
    gone_users = set()
    for i in range(0, len(uids), 100):
        batch = uids[i:i + 100]
        d = cl.get("/users", {"ids": ",".join(batch), "user.fields": USER_FIELDS}, len(batch) * cl.prices.get("user", 0))
        data = d.get("data", [])
        cl.charge("users(sync)", "user", len(data))
        alive = set()
        for u in data:
            alive.add(u["id"])
            store_user(con, u)
            if u.get("protected"):
                gone_users.add(u["id"])
        gone_users |= set(batch) - alive
    for uid in gone_users:
        con.execute("DELETE FROM posts WHERE author_id=?", (uid,))
        con.execute("DELETE FROM users WHERE id=?", (uid,))
        con.execute("DELETE FROM sample WHERE user_id=?", (uid,))
        con.execute("DELETE FROM timeline_done WHERE user_id=?", (uid,))
    con.commit()
    ids = [r[0] for r in con.execute("SELECT id FROM posts")]
    removed = 0
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        d = cl.get("/tweets", {"ids": ",".join(batch), "tweet.fields": TWEET_FIELDS}, len(batch) * cl.prices.get("post", 0))
        data = d.get("data", [])
        cl.charge("tweets(sync)", "post", len(data))
        alive = set()
        for t in data:
            alive.add(t["id"])
            con.execute("UPDATE posts SET text=?, metrics=?, fetched_at=? WHERE id=?",
                        (t.get("text"), json.dumps(t.get("public_metrics") or {}), now(), t["id"]))
        for gone in set(batch) - alive:
            con.execute("DELETE FROM posts WHERE id=?", (gone,))
            con.execute("DELETE FROM seed_hits WHERE post_id=?", (gone,))
            removed += 1
        con.commit()
    print(f"照合完了: 削除・非公開アカウント {len(gone_users)} 件、削除された投稿 {removed} 件を消去しました。"
          " 続けて py xc.py export で analysis.sqlite を作り直してください。")


def cmd_purge(yes):
    if not yes:
        print("raw.sqlite と analysis.sqlite を削除します。実行するには: py xc.py purge --yes")
        return
    for p in (RAW_DB, OUT_DB):
        if os.path.exists(p):
            os.remove(p)
            print("削除:", os.path.basename(p))


def cmd_status(con):
    q = lambda s: con.execute(s).fetchone()[0]
    print(f"検索でヒットした投稿: {q('SELECT COUNT(DISTINCT post_id) FROM seed_hits'):,}")
    print(f"抽出したアカウント:   {q('SELECT COUNT(*) FROM sample'):,}")
    print(f"取得したプロフィール: {q('SELECT COUNT(*) FROM users'):,}")
    print(f"タイムライン取得済み: {q('SELECT COUNT(*) FROM timeline_done'):,}")
    print(f"保存中の投稿（合計）: {q('SELECT COUNT(*) FROM posts'):,}")
    print(f"推定使用額:           ${q('SELECT COALESCE(SUM(usd),0) FROM ledger'):.2f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["check", "run", "count", "search", "users", "probe", "phase2", "timelines",
                                        "export", "sync", "purge", "status"])
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    if a.command == "purge":
        return cmd_purge(a.yes)
    cfg = load_config()
    con = raw_db()
    if a.command == "status":
        return cmd_status(con)
    if a.command == "export":
        return cmd_export(con, cfg)
    cl = Client(load_env(), con, cfg)
    try:
        {"check": cmd_check, "run": cmd_run, "count": cmd_count, "search": cmd_search,
         "users": cmd_users, "probe": cmd_probe,
         "phase2": cmd_phase2, "timelines": cmd_timelines}.get(a.command, lambda c, f: None)(cl, cfg)
        if a.command == "sync":
            cmd_sync(cl, cfg, a.yes)
    except BudgetExceeded as e:
        print(e)
    except KeyboardInterrupt:
        print("\n中断しました。同じコマンドを再実行すると続きから再開します。")


if __name__ == "__main__":
    main()
