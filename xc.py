#!/usr/bin/env python3
"""x_cluster: collect X posts -> local SQLite, with a pseudonymized export for analysis.

Data sources (ADR-0015):
  - Official X API (pay-per-use): daily counts, seed posts (day-proportional sample), profile fallback.
  - TwexAPI (third party, cheap): account-scoped searches only (from:account) - usual posts, weights, mentions.
    Its keyword search returned only ~20% of posts, so it is not used for seeds.

Usage (Windows):  py xc.py <command> [args] [--yes]
Commands:
  tcheck     Test the TwexAPI key and the time-window syntax
  tdiag      Coverage diagnostics: tdiag <query> <day> [10min] | tdiag from <n> | tdiag fromid
  pdiag      Keyword-search coverage of other providers vs official hourly counts:
             pdiag <socialdata|twitterapi_io|scrapebadger|apify|twex> <query> <day> | pdiag compare <query> <day>
  sdiag      Same as pdiag socialdata
  check      Test the official X API token (~$0.005)
  run        Do everything below in order (count, search, sample, users, tbaseline, tweights, tany, export)
  count      Official API: daily post counts per query (~$0.01 per query)
  search     Official API: seed posts, day-proportional sample per query
  sample     Randomly sample accounts from the seed authors of each query
  users      Profiles and @handles of sampled accounts (TwexAPI if resolve_via=twex, else official $0.01 each)
  tbaseline  TwexAPI: each sampled account's posts from before the controversy (or recent posts if new)
  tweights   TwexAPI: each sampled account's matching posts in the study period (sampling weights)
  tany       TwexAPI: did accounts in any_controversy_groups mention any controversy (H3)
  export     Build analysis.sqlite (pseudonymized) for analysis
  purge      Delete raw.sqlite and analysis.sqlite (needs --yes)
  status     Show what is stored and what has been spent

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
TWEX_API = "https://api.twexapi.io"
LOG_DIR = os.path.join(BASE, "logs")
RAW_DB = os.path.join(BASE, "raw.sqlite")
OUT_DB = os.path.join(BASE, "analysis.sqlite")
SALT_FILE = os.path.join(BASE, "salt.secret")
CONFIG = os.path.join(BASE, "config.json")

TWEET_FIELDS = "created_at,lang,referenced_tweets,in_reply_to_user_id,author_id,entities,public_metrics"
USER_FIELDS = ("created_at,description,public_metrics,protected,verified,verified_type,"
               "url,location,pinned_tweet_id,profile_image_url")
MENTION_RE = re.compile(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]{1,15})")
URL_RE = re.compile(r"https?://\S+")
AT_RE = re.compile(r"@[A-Za-z0-9_]{1,15}")

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
    for k in ("X_BEARER_TOKEN", "TWEX_API_KEY", "SOCIALDATA_API_KEY", "TWITTERAPI_IO_KEY",
              "SCRAPEBADGER_API_KEY", "APIFY_TOKEN"):
        env.setdefault(k, os.environ.get(k, ""))
    return env


def load_config():
    with open(CONFIG, encoding="utf-8-sig") as f:
        return json.load(f)


def raw_db():
    con = sqlite3.connect(RAW_DB)
    # migrate from the official-API-only layout (ADR-0011): drop old tables that have an incompatible shape
    cols = [r[1] for r in con.execute("PRAGMA table_info(probe)")]
    if cols and "any_more" not in cols:
        con.execute("DROP TABLE probe")
    for old in ("phase2", "timeline_done", "search_state", "search_day_state", "author_counts"):
        con.execute(f"DROP TABLE IF EXISTS {old}")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS posts(
        id TEXT PRIMARY KEY, author_id TEXT, created_at TEXT, text TEXT, lang TEXT,
        conversation_id TEXT, in_reply_to_user_id TEXT, referenced TEXT, mentions TEXT,
        metrics TEXT, source TEXT, fetched_at TEXT);
    CREATE INDEX IF NOT EXISTS posts_author ON posts(author_id);
    CREATE TABLE IF NOT EXISTS seed_hits(post_id TEXT, query_name TEXT, PRIMARY KEY(post_id, query_name));
    CREATE TABLE IF NOT EXISTS sample(user_id TEXT PRIMARY KEY, selected_at TEXT, query_name TEXT);
    CREATE TABLE IF NOT EXISTS users(
        id TEXT PRIMARY KEY, username TEXT, name TEXT, description TEXT, created_at TEXT,
        followers INTEGER, following INTEGER, tweet_count INTEGER, listed INTEGER,
        protected INTEGER, verified INTEGER, verified_type TEXT, url TEXT, location TEXT,
        pinned_tweet_id TEXT, profile_image_url TEXT, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS ledger(ts TEXT, endpoint TEXT, kind TEXT, units INTEGER, usd REAL);
    CREATE TABLE IF NOT EXISTS twex_ledger(ts TEXT, path TEXT, items INTEGER, credits INTEGER);
    CREATE TABLE IF NOT EXISTS daily_counts(query_name TEXT, start TEXT, end TEXT, n INTEGER,
        PRIMARY KEY(query_name, start));
    CREATE TABLE IF NOT EXISTS osearch_state(query_name TEXT, start TEXT, quota INTEGER, n INTEGER,
        next_token TEXT, done INTEGER, PRIMARY KEY(query_name, start));
    CREATE TABLE IF NOT EXISTS tsearch_state(query_name TEXT, start TEXT, quota INTEGER, n INTEGER,
        cursor TEXT, done INTEGER, PRIMARY KEY(query_name, start));
    CREATE TABLE IF NOT EXISTS baseline_state(user_id TEXT PRIMARY KEY, window TEXT, n INTEGER, has_more INTEGER,
        status TEXT, fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS probe(user_id TEXT PRIMARY KEY, match_n INTEGER, any_n INTEGER, any_more INTEGER,
        fetched_at TEXT);
    CREATE TABLE IF NOT EXISTS audit(post_id TEXT PRIMARY KEY, query_name TEXT, window_start TEXT,
        in_twex INTEGER, is_reply INTEGER, is_quote INTEGER, n_mentions INTEGER, text TEXT, author_id TEXT);
    """)
    return con


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def iso(t):
    """Normalize a timestamp to 'YYYY-MM-DDTHH:MM:SS.000Z' (UTC)."""
    if not t:
        return None
    if isinstance(t, (int, float)):
        d = dt.datetime.fromtimestamp(t / (1000 if t > 1e11 else 1), dt.timezone.utc)
    else:
        s = str(t)
        d = None
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                d = dt.datetime.strptime(s.replace("Z", "+0000"), fmt)
                break
            except ValueError:
                continue
        if d is None:
            return s
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def to_dt(s):
    return dt.datetime.strptime(iso(s), "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------- official X API

class Client:
    def __init__(self, token, con, cfg):
        if not token:
            raise SystemExit(".env に X_BEARER_TOKEN がありません（公式APIを使う手順に必要です）。")
        self.token, self.con, self.cfg = token, con, cfg
        self.prices = cfg.get("prices", {})
        self.budget = float(cfg.get("budget_usd", 0))

    def spent(self):
        return self.con.execute("SELECT COALESCE(SUM(usd),0) FROM ledger").fetchone()[0]

    def guard(self, projected):
        if self.spent() + projected > self.budget:
            raise BudgetExceeded(
                f"公式APIの予算上限 ${self.budget:.2f} に達するため停止しました（推定使用額 ${self.spent():.2f}）。"
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
                    print(f"  公式API レート制限: {wait}秒待機します…")
                    time.sleep(min(wait, 960))
                    continue
                if e.code >= 500:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise SystemExit(f"公式API HTTP {e.code} {path}\n{body}")
            except urllib.error.URLError as e:
                print(f"  通信エラー（{e.reason}）。再試行します…")
                time.sleep(5 * (attempt + 1))
        raise SystemExit(f"再試行しても失敗しました: {path}")


def count_query(cl, cfg, query, start, end):
    """Official counts: list of (start, end, n) day buckets for a query between start and end."""
    params = {"query": query, "granularity": "day", "start_time": start}
    if end:
        params["end_time"] = end
    out, pages = [], 0
    while True:
        d = cl.get("/tweets/counts/all", params, 0.02)
        cl.charge("counts/all", "count_all_request", 1)
        out += [(b["start"], b["end"], int(b.get("tweet_count", 0))) for b in d.get("data", [])]
        pages += 1
        nt = d.get("meta", {}).get("next_token")
        if not nt or pages >= 20:
            break
        params["next_token"] = nt
        time.sleep(1.1)
    return out


# ---------------------------------------------------------------- TwexAPI

class Twex:
    """Minimal TwexAPI client (advanced search only). 5 credits per returned item or per empty request."""

    def __init__(self, key, con, cfg):
        if not key:
            raise SystemExit(".env に TWEX_API_KEY がありません。TwexAPI のダッシュボードで発行したキーを書いてください。")
        self.key, self.con, self.cfg = key, con, cfg
        # adaptive pacing: start slightly slower than the plan's QPS, back off on 429, creep back on success
        self.base_interval = 1.15 / float(cfg.get("twex_qps", 1))
        self.min_interval = self.base_interval
        self.max_credits = int(cfg.get("twex_max_credits", 0))
        self.last = 0.0
        self.n429 = 0
        self.n_req = 0
        self.t0 = time.time()

    def pace(self):
        """Human-readable pacing info for progress lines."""
        mins = max(1e-9, (time.time() - self.t0) / 60)
        return (f"{self.n_req / mins:.0f}回/分（間隔 {self.min_interval:.1f}秒、429 累計 {self.n429} 回）"
                f"  クレジット使用 {self.used():,}")

    def used(self):
        return self.con.execute("SELECT COALESCE(SUM(credits),0) FROM twex_ledger").fetchone()[0]

    def _req(self, method, path, body=None):
        for attempt in range(60):  # 429s are retried with growing intervals; other errors give up much sooner
            wait = self.last + self.min_interval - time.time()
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            req = urllib.request.Request(
                TWEX_API + path, method=method,
                data=json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None,
                headers={"Authorization": f"Bearer {self.key}", "Content-Type": "application/json",
                         "User-Agent": "x_cluster/1.0"})
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    d = json.load(r)
            except urllib.error.HTTPError as e:
                msg = e.read().decode("utf-8", "replace")[:600]
                if e.code == 429:
                    self._slow_down(e.headers.get("Retry-After"), msg)
                    continue
                if e.code >= 500 and attempt < 8:
                    print(f"  TwexAPI {e.code}: {10 * (attempt + 1)}秒待って再試行します…")
                    time.sleep(10 * (attempt + 1))
                    continue
                if e.code in (401, 403):
                    raise SystemExit(f"TwexAPI の認証エラー（{e.code}）。.env の TWEX_API_KEY を確認してください。\n{msg}")
                if e.code == 402:
                    raise SystemExit("TwexAPI のクレジットが足りません。ダッシュボードで残高を確認してください。")
                raise SystemExit(f"TwexAPI HTTP {e.code} {path}\n{msg}")
            except urllib.error.URLError as e:
                print(f"  通信エラー（{e.reason}）。再試行します…")
                time.sleep(5 * (attempt + 1))
                continue
            code = d.get("code") if isinstance(d, dict) else None
            if code not in (None, 200):
                if code == 429:
                    self._slow_down(None, d.get("msg"))
                    continue
                raise SystemExit(f"TwexAPI エラー code={code} msg={d.get('msg')}")
            # success: speed back up gradually toward the plan's rate
            self.min_interval = max(self.base_interval, self.min_interval * 0.95)
            self.n_req += 1
            return d
        raise SystemExit(f"TwexAPI: 再試行しても失敗しました: {path}")

    def _slow_down(self, retry_after, msg=None):
        self.n429 += 1
        self.min_interval = min(self.min_interval * 1.6, 12.0)
        try:
            wait = float(retry_after) if retry_after else self.min_interval * 2
        except ValueError:
            wait = self.min_interval * 2
        if self.n429 == 1 or self.n429 % 20 == 0:
            print(f"  TwexAPI の回数制限（429、累計 {self.n429} 回）。間隔を {self.min_interval:.1f} 秒に広げて続けます。"
                  f"\n    TwexAPI からのメッセージ: {(msg or '').strip()[:200]}")
        time.sleep(wait)

    def balance(self):
        return self._req("GET", "/balance")

    def search_page(self, term, cursor=None, sort="Latest"):
        if self.max_credits and self.used() + 100 > self.max_credits:
            raise BudgetExceeded(f"TwexAPI のクレジット上限 {self.max_credits:,} に達するため停止しました"
                                 f"（使用 {self.used():,}）。config.json の twex_max_credits を見直してください。")
        body = {"searchTerms": [term], "sortBy": sort}
        if cursor:
            body["next_cursor"] = cursor
        d = self._req("POST", "/twitter/advanced_search/page", body)
        data = d.get("data") or []
        self.con.execute("INSERT INTO twex_ledger VALUES(?,?,?,?)",
                         (now(), "advanced_search", len(data), 5 * max(1, len(data))))
        nxt = d.get("next_cursor") if d.get("has_next_page") else None
        return data, (nxt or None)


def web_query(q):
    """Convert an official-API query to X web-search syntax (used by TwexAPI)."""
    return q.replace("-is:retweet", "-filter:retweets").replace("is:retweet", "filter:retweets")


def window_ops(cfg, start, end):
    """Time-window operators. 'unix' uses since_time/until_time; 'date_utc' uses since:/until: with UTC times."""
    s, e = to_dt(start), to_dt(end)
    if cfg.get("twex_time_format", "unix") == "unix":
        return f"since_time:{int(s.timestamp())} until_time:{int(e.timestamp())}"
    return f"since:{s:%Y-%m-%d_%H:%M:%S}_UTC until:{e:%Y-%m-%d_%H:%M:%S}_UTC"


def twex_norm(t):
    """Normalize a TwexAPI tweet object to our post dict + author dict."""
    u = t.get("user") or t.get("author") or {}
    text = t.get("full_text") or t.get("text") or ""
    refs = []
    irt = t.get("in_reply_to")
    if isinstance(irt, dict):
        irt = irt.get("tweet_id") or irt.get("id")
    if irt:
        refs.append({"type": "replied_to", "id": str(irt)})
    q = t.get("quoted_status_id_str")
    if not q and isinstance(t.get("quote"), dict):
        q = t["quote"].get("tweet_id") or t["quote"].get("id")
    if q:
        refs.append({"type": "quoted", "id": str(q)})
    post = {
        "id": str(t.get("tweet_id") or t.get("id")),
        "author_id": str(u.get("id")) if u.get("id") else None,
        "created_at": iso(t.get("created_at_datetime") or t.get("created_at")),
        "text": text, "lang": t.get("lang"), "conversation_id": None,
        "in_reply_to_user_id": str(t["in_reply_to_user_id"]) if t.get("in_reply_to_user_id") else None,
        "referenced_tweets": refs,
        "mentions": [{"id": None, "username": m} for m in MENTION_RE.findall(text)],
        "public_metrics": {"like_count": t.get("favorite_count"), "retweet_count": t.get("retweet_count"),
                           "reply_count": t.get("reply_count"), "quote_count": t.get("quote_count")},
    }
    return post, u


# ---------------------------------------------------------------- storage helpers

def store_post(con, t, source):
    mentions = t.get("mentions")
    if mentions is None:
        mentions = [{"id": m.get("id"), "username": m.get("username")}
                    for m in (t.get("entities") or {}).get("mentions", [])]
    con.execute("""INSERT INTO posts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET metrics=excluded.metrics, fetched_at=excluded.fetched_at,
        source=CASE WHEN (','||posts.source||',') LIKE ('%,'||excluded.source||',%') THEN posts.source
                    ELSE posts.source||','||excluded.source END""",
        (t["id"], t.get("author_id"), t.get("created_at"), t.get("text"), t.get("lang"),
         t.get("conversation_id"), t.get("in_reply_to_user_id"),
         json.dumps(t.get("referenced_tweets") or []), json.dumps(mentions),
         json.dumps(t.get("public_metrics") or {}), source, now()))


def store_twex_user(con, u):
    if not u.get("id"):
        return
    pinned = u.get("pinned_tweet_ids") or []
    img = "default_profile" if u.get("default_profile_image") else (u.get("profile_image_url") or "")
    urls = u.get("urls") or []
    con.execute("""INSERT OR REPLACE INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (str(u["id"]), u.get("screen_name"), u.get("name"), u.get("description"),
         iso(u.get("created_at_datetime") or u.get("created_at")),
         u.get("followers_count"), u.get("following_count", u.get("friends_count")),
         u.get("statuses_count"), u.get("listed_count"), int(bool(u.get("protected"))),
         int(bool(u.get("is_verified") or u.get("is_blue_verified"))), u.get("verified_type"),
         (urls[0].get("expanded_url") if urls and isinstance(urls[0], dict) else None),
         u.get("location"), str(pinned[0]) if pinned else None, img, now()))


def store_official_user(con, u):
    pm = u.get("public_metrics") or {}
    con.execute("""INSERT OR REPLACE INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (u["id"], u.get("username"), u.get("name"), u.get("description"), iso(u.get("created_at")),
         pm.get("followers_count"), pm.get("following_count"), pm.get("tweet_count"),
         pm.get("listed_count"), int(bool(u.get("protected"))), int(bool(u.get("verified"))),
         u.get("verified_type"), u.get("url"), u.get("location"), u.get("pinned_tweet_id"),
         u.get("profile_image_url"), now()))


def store_twex_page(con, data, source, query_name=None):
    n = 0
    for t in data:
        post, u = twex_norm(t)
        if not post["id"] or post["id"] == "None":
            continue
        store_post(con, post, source)
        store_twex_user(con, u)
        if query_name:
            con.execute("INSERT OR IGNORE INTO seed_hits VALUES(?,?)", (post["id"], query_name))
        n += 1
    return n


# ---------------------------------------------------------------- helpers

def n_accounts(cfg, q):
    return int(q.get("n_accounts", cfg.get("n_accounts_default", 200)))


def seed_cap(cfg, q):
    return int(q.get("max_seed_posts", cfg.get("max_seed_posts_per_query", 1000)))


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


def need_counts(con, cfg):
    have = {r[0] for r in con.execute("SELECT DISTINCT query_name FROM daily_counts")}
    return [q for q in cfg["queries"] if q["name"] not in have]


# ---------------------------------------------------------------- commands: checks

def cmd_check(env, con, cfg):
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    d = cl.get("/tweets/counts/recent", {"query": "の lang:ja -is:retweet", "granularity": "day"}, 0.01)
    cl.charge("counts/recent", "count_request", 1)
    print("公式APIのトークンは有効です。直近7日間で「の」を含む日本語投稿数（RT除く）:",
          f"{d.get('meta', {}).get('total_tweet_count', '?'):,}")


def cmd_tcheck(env, con, cfg):
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    try:
        print("TwexAPI 残高:", json.dumps(tw.balance(), ensure_ascii=False)[:300])
    except SystemExit as e:
        print("残高の取得に失敗しました（検索は試します）:", str(e)[:200])
    s = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    e = s + dt.timedelta(hours=1)
    base = "刀剣乱舞 lang:ja -filter:retweets"
    for fmt in ("unix", "date_utc"):
        term = f"{base} {window_ops(dict(cfg, twex_time_format=fmt), s.isoformat(), e.isoformat())}"
        data, nxt = tw.search_page(term)
        times = sorted(iso(t.get("created_at_datetime") or t.get("created_at")) or "" for t in data)
        inside = all(s.strftime("%Y-%m-%dT%H") <= x[:13] <= e.strftime("%Y-%m-%dT%H") for x in times if x)
        print(f"[{fmt}] {term}\n  件数 {len(data)}（次のページ {'あり' if nxt else 'なし'}）"
              f" 時刻の範囲 {times[0] if times else '-'} 〜 {times[-1] if times else '-'}  窓内: {'OK' if inside else 'NG'}")
        if data:
            print("  投稿の項目:", ", ".join(sorted(data[0].keys()))[:600])
            u = data[0].get("user") or data[0].get("author") or {}
            print("  投稿者の項目:", ", ".join(sorted(u.keys()))[:600])
    con.commit()
    print(f"使用クレジット（推定）: {tw.used():,}")


# ---------------------------------------------------------------- commands: collection

def cmd_count(env, con, cfg):
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    total = 0
    for q in cfg["queries"]:
        buckets = count_query(cl, cfg, q["query"], cfg["start_time"], cfg.get("end_time"))
        con.execute("DELETE FROM daily_counts WHERE query_name=?", (q["name"],))
        con.executemany("INSERT OR REPLACE INTO daily_counts VALUES(?,?,?,?)",
                        [(q["name"], s, e, n) for s, e, n in buckets])
        con.commit()
        n = sum(b[2] for b in buckets)
        total += n
        plan = min(n, seed_cap(cfg, q))
        print(f"[{q['name']}] 公式の件数 {n:,} 件 → 公式APIで日別比例に {plan:,} 件を集める予定")
    print(f"合計 {total:,} 件。公式API 推定使用額 ${cl.spent():.2f}")


def cmd_search(env, con, cfg):
    """Seed posts via the official API, stratified by day in proportion to daily counts (ADR-0009, ADR-0015)."""
    if need_counts(con, cfg):
        cmd_count(env, con, cfg)
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    price = cl.prices.get("post", 0)
    for q in cfg["queries"]:
        buckets = con.execute("SELECT start, end, n FROM daily_counts WHERE query_name=? ORDER BY start",
                              (q["name"],)).fetchall()
        for (st, e, n), quota in zip(buckets, allocate(seed_cap(cfg, q), buckets)):
            con.execute("INSERT OR IGNORE INTO osearch_state VALUES(?,?,?,?,?,?)", (q["name"], st, quota, 0, None, 0))
        con.commit()
        ends = {st: e for st, e, _ in buckets}
        todo = con.execute("SELECT start, quota, n, next_token FROM osearch_state WHERE query_name=? AND done=0 "
                           "AND quota>0 ORDER BY start", (q["name"],)).fetchall()
        for st, quota, got, token in todo:
            while got < quota:
                params = {"query": q["query"], "max_results": max(10, min(100, quota - got)),
                          "tweet.fields": TWEET_FIELDS, "start_time": st, "end_time": ends[st]}
                if token:
                    params["next_token"] = token
                d = cl.get("/tweets/search/all", params, 100 * price)
                data = d.get("data", [])
                cl.charge("search/all", "post", len(data))
                for t in data[:quota - got]:
                    store_post(con, t, "seed")
                    con.execute("INSERT OR IGNORE INTO seed_hits VALUES(?,?)", (t["id"], q["name"]))
                got += min(len(data), quota - got)
                token = d.get("meta", {}).get("next_token")
                done = int(got >= quota or not token)
                con.execute("UPDATE osearch_state SET n=?, next_token=?, done=? WHERE query_name=? AND start=?",
                            (got, token, done, q["name"], st))
                con.commit()
                time.sleep(1.1)  # full-archive search: 1 request/sec
                if done:
                    break
        n_all = con.execute("SELECT COALESCE(SUM(n),0) FROM osearch_state WHERE query_name=?", (q["name"],)).fetchone()[0]
        print(f"[{q['name']}] 起点の投稿 {n_all:,} 件（日別に比例配分）  公式API 推定使用額 ${cl.spent():.2f}")
    print("検索完了。")


def cmd_sample(env, con, cfg):
    if con.execute("SELECT 1 FROM sample LIMIT 1").fetchone():
        print("抽出済みです。")
        return
    rng = random.Random(int(cfg.get("random_seed", 42)))
    chosen = set()
    for q in cfg["queries"]:
        pool = [r[0] for r in con.execute(
            "SELECT DISTINCT p.author_id FROM posts p JOIN seed_hits s ON s.post_id=p.id "
            "LEFT JOIN users u ON u.id=p.author_id WHERE s.query_name=? AND p.author_id IS NOT NULL "
            "AND COALESCE(u.protected,0)=0 ORDER BY p.author_id", (q["name"],))]
        free = [a for a in pool if a not in chosen]
        k = n_accounts(cfg, q)
        take = free if len(free) <= k else rng.sample(free, k)
        chosen.update(take)
        con.executemany("INSERT OR IGNORE INTO sample(user_id, selected_at, query_name) VALUES(?,?,?)",
                        [(a, now(), q["name"]) for a in take])
        print(f"  [{q['name']}] 投稿者 {len(pool):,} 人 → {len(take):,} 人を無作為抽出")
    con.commit()
    print(f"合計 {len(chosen):,} 人。")


def cmd_users(env, con, cfg):
    """Profiles (incl. @handle) of sampled accounts. resolve_via=twex: TwexAPI 'from:<id>' search (cheap);
    otherwise, or for accounts TwexAPI could not resolve, the official users lookup ($0.01 each)."""
    todo = [r[0] for r in con.execute(
        "SELECT user_id FROM sample WHERE user_id NOT IN (SELECT id FROM users WHERE username IS NOT NULL)")]
    if cfg.get("resolve_via") == "twex" and todo:
        tw = Twex(env["TWEX_API_KEY"], con, cfg)
        w = window_ops(cfg, cfg["start_time"], cfg.get("end_time") or now())
        for i, uid in enumerate(todo, 1):
            data, _ = tw.search_page(f"from:{uid} -filter:retweets {w}")
            for t in data:
                u = t.get("user") or t.get("author") or {}
                if str(u.get("id")) == uid:
                    store_twex_user(con, u)
                    break
            con.commit()
            if i % 50 == 0 or i == len(todo):
                print(f"プロフィール（TwexAPI） {i:,}/{len(todo):,}  {tw.pace()}")
        todo = [r[0] for r in con.execute(
            "SELECT user_id FROM sample WHERE user_id NOT IN (SELECT id FROM users WHERE username IS NOT NULL)")]
    if not todo:
        print("プロフィール取得完了。")
        return
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    for i in range(0, len(todo), 100):
        batch = todo[i:i + 100]
        d = cl.get("/users", {"ids": ",".join(batch), "user.fields": USER_FIELDS}, len(batch) * cl.prices.get("user", 0))
        data = d.get("data", [])
        cl.charge("users", "user", len(data))
        for u in data:
            store_official_user(con, u)
        con.commit()
        print(f"プロフィール（公式API） {min(i + 100, len(todo)):,}/{len(todo):,}  公式API 推定使用額 ${cl.spent():.2f}")
    print("プロフィール取得完了。")


def cmd_tbaseline(env, con, cfg):
    """Each sampled account's posts before the controversy; recent posts if the account is newer than that."""
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    bs, be = cfg["baseline_start"], cfg["baseline_end"]
    bmax, rmax = int(cfg.get("baseline_max_posts", 50)), int(cfg.get("recent_max_posts", 30))
    todo = con.execute("""SELECT s.user_id, u.username, u.created_at FROM sample s JOIN users u ON u.id=s.user_id
        WHERE s.user_id NOT IN (SELECT user_id FROM baseline_state)""").fetchall()
    for i, (uid, sn, created) in enumerate(todo, 1):
        if not sn:
            con.execute("INSERT OR REPLACE INTO baseline_state VALUES(?,?,?,?,?,?)", (uid, None, 0, 0, "no_name", now()))
            continue
        new = bool(created) and iso(created) >= iso(be)
        window, src, cap = ("recent", "recent", rmax) if new else ("baseline", "baseline", bmax)
        w = window_ops(cfg, cfg["start_time"], now()) if new else window_ops(cfg, bs, be)
        term = f"from:{sn} -filter:retweets {w}"
        got, cursor, more = 0, None, 0
        while got < cap:
            data, cursor = tw.search_page(term, cursor)
            data = data[:cap - got]
            got += store_twex_page(con, data, src)
            if not cursor or not data:
                break
        more = int(bool(cursor) and got >= cap)
        con.execute("INSERT OR REPLACE INTO baseline_state VALUES(?,?,?,?,?,?)",
                    (uid, window, got, more, "ok" if got else "empty", now()))
        con.commit()
        if i % 25 == 0 or i == len(todo):
            print(f"ふだんの投稿 {i:,}/{len(todo):,}  {tw.pace()}")


def cmd_tany(env, con, cfg):
    """H3: did each account in any_controversy_groups mention any of the controversies in the study period?"""
    groups = cfg.get("any_controversy_groups", [])
    if not groups:
        return
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    w = window_ops(cfg, cfg["start_time"], cfg.get("end_time") or now())
    todo = con.execute(f"""SELECT s.user_id, u.username FROM sample s JOIN users u ON u.id=s.user_id
        WHERE s.query_name IN ({','.join('?' * len(groups))})
        AND s.user_id NOT IN (SELECT user_id FROM probe WHERE any_n IS NOT NULL)""", groups).fetchall()
    for i, (uid, sn) in enumerate(todo, 1):
        if not sn:
            continue
        data, cursor = tw.search_page(f"from:{sn} ({web_query(cfg['any_controversy_query'])}) -filter:retweets {w}")
        store_twex_page(con, data, "any")
        con.execute("INSERT INTO probe(user_id, any_n, any_more, fetched_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(user_id) DO UPDATE SET any_n=excluded.any_n, any_more=excluded.any_more",
                    (uid, len(data), int(bool(cursor)), now()))
        con.commit()
        if i % 50 == 0 or i == len(todo):
            print(f"言及の確認 {i:,}/{len(todo):,}  {tw.pace()}")


def cmd_tweights(env, con, cfg):
    """Sampling weights (ADR-0009): each sampled account's posts matching its group's query in the study period,
    counted with TwexAPI 'from:' searches (account-scoped searches are near-complete, ADR-0015). Capped."""
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    cap = int(cfg.get("weight_count_cap", 200))
    qmap = {q["name"]: web_query(q["query"]) for q in cfg["queries"]}
    w = window_ops(cfg, cfg["start_time"], cfg.get("end_time") or now())
    todo = con.execute("""SELECT s.user_id, s.query_name, u.username FROM sample s JOIN users u ON u.id=s.user_id
        WHERE s.user_id NOT IN (SELECT user_id FROM probe WHERE match_n IS NOT NULL)""").fetchall()
    for i, (uid, qn, sn) in enumerate(todo, 1):
        if not sn or qn not in qmap:
            continue
        n, cursor, more = 0, None, 0
        while n < cap:
            data, cursor = tw.search_page(f"from:{sn} ({qmap[qn]}) {w}", cursor)
            n += len(data)
            if not cursor or not data:
                break
        more = int(bool(cursor) and n >= cap)
        con.execute("INSERT INTO probe(user_id, match_n, any_more, fetched_at) VALUES(?,?,NULL,?) "
                    "ON CONFLICT(user_id) DO UPDATE SET match_n=excluded.match_n", (uid, min(n, cap), now()))
        con.commit()
        if i % 50 == 0 or i == len(todo):
            print(f"重み用の件数 {i:,}/{len(todo):,}  {tw.pace()}")


def cmd_tdiag(env, con, cfg, args=()):
    """Coverage diagnosis for one query and day: official hourly counts vs TwexAPI with
    (A) one 6-hour window and (B) six 1-hour windows. Usage: py xc.py tdiag regista 2026-11-05"""
    if args and args[0] == "from":
        return tdiag_from(env, con, cfg, int(args[1]) if len(args) > 1 else 10)
    if args and args[0] == "fromid":
        return tdiag_fromid(env, con, cfg)
    qname = args[0] if args else cfg["queries"][0]["name"]
    day = args[1] if len(args) > 1 else None
    if len(args) > 2 and args[2] == "10min":
        return tdiag_10min(env, con, cfg, qname, day)
    q = next(x for x in cfg["queries"] if x["name"] == qname)
    if not day:
        day = con.execute("SELECT start FROM daily_counts WHERE query_name=? ORDER BY n DESC LIMIT 1",
                          (qname,)).fetchone()[0][:10]
    start = dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    d = cl.get("/tweets/counts/all", {"query": q["query"], "granularity": "hour",
                                      "start_time": iso(start.isoformat()),
                                      "end_time": iso((start + dt.timedelta(hours=6)).isoformat())}, 0.02)
    cl.charge("counts/all(diag)", "count_all_request", 1)
    hourly = [int(b.get("tweet_count", 0)) for b in d.get("data", [])]
    print(f"[{qname}] {day} 0〜6時（UTC）の公式の件数: {sum(hourly):,}（1時間ごと {hourly}）")
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    term0 = web_query(q["query"])

    def pull(s, e):
        ids, cursor, pages, why = set(), None, 0, ""
        while True:
            data, cursor = tw.search_page(f"{term0} {window_ops(cfg, s.isoformat(), e.isoformat())}", cursor)
            pages += 1
            ids |= {str(t.get("id") or t.get("tweet_id")) for t in data}
            if not data:
                why = "空のページで終了"
                break
            if not cursor:
                why = "次のページなしで終了"
                break
        return ids, pages, why

    a, pages, why = pull(start, start + dt.timedelta(hours=6))
    print(f"(A) 6時間の枠1つ: {len(a):,} 件（{pages} ページ、{why}）  {tw.pace()}")
    b = set()
    for h in range(6):
        s = start + dt.timedelta(hours=h)
        ids, pages, why = pull(s, s + dt.timedelta(hours=1))
        b |= ids
        print(f"    {h}時台: {len(ids):,} 件（公式 {hourly[h] if h < len(hourly) else '?'}、{pages} ページ、{why}）")
    print(f"(B) 1時間の枠6つ: {len(b):,} 件  {tw.pace()}")
    print(f"取れた割合: (A) {len(a) / max(1, sum(hourly)):.0%}  (B) {len(b) / max(1, sum(hourly)):.0%}")
    con.commit()


def tdiag_pull(tw, term, s, e, cfg, cap=None):
    ids, cursor, pages = set(), None, 0
    while True:
        data, cursor = tw.search_page(f"{term} {window_ops(cfg, s.isoformat(), e.isoformat())}", cursor)
        pages += 1
        ids |= {str(t.get("id") or t.get("tweet_id")) for t in data}
        if not data or not cursor or (cap and len(ids) >= cap):
            return ids, pages


def tdiag_10min(env, con, cfg, qname, day):
    """Coverage of the first hour of a day split into six 10-minute windows."""
    q = next(x for x in cfg["queries"] if x["name"] == qname)
    start = dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    term = web_query(q["query"])
    tot_o, tot_t = 0, 0
    for k in range(6):
        s = start + dt.timedelta(minutes=10 * k)
        e = s + dt.timedelta(minutes=10)
        d = cl.get("/tweets/counts/all", {"query": q["query"], "granularity": "minute",
                                          "start_time": iso(s.isoformat()), "end_time": iso(e.isoformat())}, 0.02)
        cl.charge("counts/all(diag)", "count_all_request", 1)
        o = sum(int(b.get("tweet_count", 0)) for b in d.get("data", []))
        ids, pages = tdiag_pull(tw, term, s, e, cfg)
        tot_o, tot_t = tot_o + o, tot_t + len(ids)
        print(f"  {s:%H:%M}〜{e:%H:%M}: TwexAPI {len(ids)} 件 / 公式 {o} 件（{pages} ページ）")
        time.sleep(1.1)
    print(f"10分区切り: {tot_t} / {tot_o} 件（{tot_t / max(1, tot_o):.0%}）  {tw.pace()}")
    con.commit()


def tdiag_fromid(env, con, cfg):
    """Does TwexAPI accept 'from:<numeric user id>'? If yes, set resolve_via=twex to skip the official users lookup."""
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    rows = con.execute("SELECT DISTINCT u.id, u.username FROM users u JOIN posts p ON p.author_id=u.id "
                       "WHERE u.username IS NOT NULL LIMIT 3").fetchall()
    w = window_ops(cfg, cfg["start_time"], now())
    ok = 0
    for i, (uid, sn) in enumerate(rows, 1):
        data, _ = tw.search_page(f"from:{uid} -filter:retweets {w}")
        hit = any(str((t.get("user") or {}).get("id")) == str(uid) for t in data)
        ok += hit
        print(f"  アカウント{i}: from:<ID> で {len(data)} 件、本人の投稿 {'あり' if hit else 'なし'}")
    print("from:<ID> は使えます。config.json の resolve_via を \"twex\" にしてください。" if ok == len(rows) and rows
          else "from:<ID> は使えません。resolve_via は \"official\" のまま（公式APIで1人0.01ドル）にします。")
    con.commit()


def tdiag_from(env, con, cfg, n):
    """Coverage of 'from:account' searches in the baseline window, for n random seed authors."""
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    tw = Twex(env["TWEX_API_KEY"], con, cfg)
    rng = random.Random(99)
    rows = con.execute("SELECT DISTINCT u.id, u.username FROM users u JOIN posts p ON p.author_id=u.id "
                       "JOIN seed_hits s ON s.post_id=p.id WHERE u.username IS NOT NULL AND COALESCE(u.protected,0)=0 "
                       "ORDER BY u.id").fetchall()
    bs, be = to_dt(cfg["baseline_start"]), to_dt(cfg["baseline_end"])
    tot_o, tot_t, k = 0, 0, 0
    for uid, sn in rng.sample(rows, min(n * 3, len(rows))):
        o = sum(b[2] for b in count_query(cl, cfg, f"from:{uid} -is:retweet", cfg["baseline_start"], cfg["baseline_end"]))
        if o == 0:
            continue
        cap = min(o, 100)
        ids, pages = tdiag_pull(tw, f"from:{sn} -filter:retweets", bs, be, cfg, cap=cap)
        got = min(len(ids), cap)
        tot_o, tot_t, k = tot_o + cap, tot_t + got, k + 1
        print(f"  アカウント{k}: TwexAPI {got} 件 / 公式 {o} 件（比べた上限 {cap}、{pages} ページ）")
        if k >= n:
            break
    print(f"ふだんの投稿（from:）: {tot_t} / {tot_o} 件（{tot_t / max(1, tot_o):.0%}）  {tw.pace()}"
          f"  公式API 推定使用額 ${cl.spent():.2f}")
    con.commit()


# ---------------------------------------------------------------- keyword-search coverage across providers (pdiag)

class Provider:
    """Common pacing and HTTP for third-party search APIs used only by pdiag. Posts are not stored, only IDs."""
    name, key_env, label = "", "", ""
    price_per_item = 0.0

    def __init__(self, env, cfg):
        self.key = env.get(self.key_env) or os.environ.get(self.key_env, "")
        if not self.key:
            raise SystemExit(f".env に {self.key_env} がありません。{self.label} で発行したキーを書いてください。")
        self.interval = float(cfg.get("pdiag_interval", {}).get(self.name, self.default_interval))
        self.last = 0.0
        self.n_req = 0
        self.n_items = 0

    default_interval = 1.0

    def http(self, url, headers, body=None, timeout=120):
        for attempt in range(10):
            wait = self.last + self.interval - time.time()
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
            h = {"Accept": "application/json", "User-Agent": "x_cluster/1.0", **headers}
            if data is not None:
                h["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=h, method="POST" if data is not None else "GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    self.n_req += 1
                    return json.load(r)
            except urllib.error.HTTPError as e:
                msg = e.read().decode("utf-8", "replace")[:300]
                if e.code == 429:
                    self.interval = min(self.interval * 1.5 + 1, 60)
                    print(f"  {self.label} の回数制限（429）。間隔を {self.interval:.0f} 秒に広げて再試行します…")
                    time.sleep(self.interval)
                    continue
                if e.code >= 500 and attempt < 5:
                    print(f"  {self.label} {e.code}: {10 * (attempt + 1)}秒待って再試行します…")
                    time.sleep(10 * (attempt + 1))
                    continue
                if e.code in (401, 403):
                    raise SystemExit(f"{self.label} の認証エラー（{e.code}）。.env の {self.key_env} を確認してください。\n{msg}")
                if e.code == 402:
                    raise SystemExit(f"{self.label}: 残高不足（402）。ダッシュボードで残高と無料枠の条件を確認してください。\n{msg}")
                raise SystemExit(f"{self.label} HTTP {e.code}\n{msg}")
            except urllib.error.URLError as e:
                print(f"  通信エラー（{e.reason}）。再試行します…")
                time.sleep(5 * (attempt + 1))
        raise SystemExit(f"{self.label}: 再試行しても失敗しました")

    def page(self, term, cursor):
        """Return (list of tweet ids, next cursor or None)."""
        raise NotImplementedError

    def window(self, term, s, e, official_n):
        w = f"since_time:{int(s.timestamp())} until_time:{int(e.timestamp())}"
        ids, cursor, pages = set(), None, 0
        while True:
            got, cursor = self.page(f"{term} {w}", cursor)
            pages += 1
            new = set(got) - ids
            ids |= new
            self.n_items += len(got)
            if not got:
                return ids, pages, "空のページで終了"
            if not cursor:
                return ids, pages, "次のページなしで終了"
            if not new:
                return ids, pages, "新しい投稿が返らなくなり終了"

    def pace(self):
        return f"リクエスト {self.n_req} 回、取得 {self.n_items:,} 件（有料なら約 ${self.n_items * self.price_per_item:.2f}）"


class PSocialData(Provider):
    name, key_env, label, price_per_item, default_interval = (
        "socialdata", "SOCIALDATA_API_KEY", "SocialData", 0.0002, 21.0)  # free: 3 requests/minute

    def page(self, term, cursor):
        p = {"query": term, "type": "Latest"}
        if cursor:
            p["cursor"] = cursor
        d = self.http(f"https://api.socialdata.tools/twitter/search?{urllib.parse.urlencode(p)}",
                      {"Authorization": f"Bearer {self.key}"})
        return [str(t.get("id_str") or t.get("id")) for t in d.get("tweets") or []], d.get("next_cursor") or None


class PTwitterApiIo(Provider):
    name, key_env, label, price_per_item, default_interval = (
        "twitterapi_io", "TWITTERAPI_IO_KEY", "TwitterAPI.io", 0.00015, 5.5)  # free credits: ~1 request / 5 s

    def page(self, term, cursor):
        p = {"query": term, "queryType": "Latest", "cursor": cursor or ""}
        d = self.http(f"https://api.twitterapi.io/twitter/tweet/advanced_search?{urllib.parse.urlencode(p)}",
                      {"X-API-Key": self.key})
        nxt = d.get("next_cursor") if d.get("has_next_page") else None
        return [str(t.get("id")) for t in d.get("tweets") or []], nxt or None


class PScrapeBadger(Provider):
    name, key_env, label, price_per_item, default_interval = (
        "scrapebadger", "SCRAPEBADGER_API_KEY", "ScrapeBadger", 0.0001, 1.0)

    def page(self, term, cursor):
        p = {"query": term, "query_type": "Latest", "count": 100}
        if cursor:
            p["cursor"] = cursor
        d = self.http(f"https://scrapebadger.com/v1/twitter/tweets/advanced_search?{urllib.parse.urlencode(p)}",
                      {"x-api-key": self.key})
        return [str(t.get("id")) for t in d.get("data") or []], d.get("next_cursor") or None


class PApify(Provider):
    """Apify actor run synchronously; one call per window (the actor pages internally)."""
    name, key_env, label, price_per_item, default_interval = (
        "apify", "APIFY_TOKEN", "Apify", 0.00025, 2.0)

    def window(self, term, s, e, official_n):
        actor = self.cfg_actor
        w = f"since_time:{int(s.timestamp())} until_time:{int(e.timestamp())}"
        cap = max(100, official_n * 2)
        body = {"searchTerms": [f"{term} {w}"], "searchMode": "Latest", "maxItems": cap, "includeReplies": True}
        items = self.http(f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?"
                          f"{urllib.parse.urlencode({'token': self.key, 'timeout': 280})}", {}, body, timeout=300)
        if isinstance(items, dict):  # error object
            raise SystemExit(f"Apify: {str(items)[:300]}")
        ids = {str(t.get("id")) for t in items if isinstance(t, dict) and t.get("id")}
        self.n_items += len(items)
        why = f"上限 {cap} 件に到達" if len(items) >= cap else "実行完了"
        return ids, 1, why


PROVIDERS = {p.name: p for p in (PSocialData, PTwitterApiIo, PScrapeBadger, PApify)}


class PTwex(Provider):
    """Wrap the existing TwexAPI client so it can be compared in the same table."""
    name, label = "twex", "TwexAPI"

    def __init__(self, env, cfg, con):
        self.tw = Twex(env["TWEX_API_KEY"], con, cfg)
        self.n_req, self.n_items = 0, 0

    def page(self, term, cursor):
        data, nxt = self.tw.search_page(term, cursor)
        self.n_req += 1
        return [str(t.get("id") or t.get("tweet_id")) for t in data], nxt

    def pace(self):
        return self.tw.pace()


def pdiag_tables(con):
    con.execute("PRAGMA busy_timeout=30000")  # several pdiag runs may write at once
    con.executescript("""
    CREATE TABLE IF NOT EXISTS diag_counts(query_name TEXT, window_start TEXT, n INTEGER,
        PRIMARY KEY(query_name, window_start));
    CREATE TABLE IF NOT EXISTS diag_ids(provider TEXT, query_name TEXT, window_start TEXT, post_id TEXT,
        PRIMARY KEY(provider, query_name, window_start, post_id));
    CREATE TABLE IF NOT EXISTS diag_official(query_name TEXT, window_start TEXT, post_id TEXT, author_id TEXT,
        is_reply INTEGER, PRIMARY KEY(query_name, post_id));
    CREATE TABLE IF NOT EXISTS diag_runs(provider TEXT, query_name TEXT, window_start TEXT, n_got INTEGER,
        pages INTEGER, why TEXT, ts TEXT, PRIMARY KEY(provider, query_name, window_start));
    """)


def pdiag_hourly(env, con, cfg, q, start):
    """Official hourly counts for UTC 0-6h of the day; cached so parallel runs pay once (~$0.01)."""
    keys = [iso((start + dt.timedelta(hours=h)).isoformat()) for h in range(6)]
    rows = dict(con.execute(f"SELECT window_start, n FROM diag_counts WHERE query_name=? AND window_start IN "
                            f"({','.join('?' * 6)})", (q["name"], *keys)).fetchall())
    if len(rows) == 6:
        return [rows[k] for k in keys]
    cl = Client(env["X_BEARER_TOKEN"], con, cfg)
    d = cl.get("/tweets/counts/all", {"query": q["query"], "granularity": "hour", "start_time": keys[0],
                                      "end_time": iso((start + dt.timedelta(hours=6)).isoformat())}, 0.02)
    cl.charge("counts/all(pdiag)", "count_all_request", 1)
    hourly = [int(b.get("tweet_count", 0)) for b in d.get("data", [])][:6]
    hourly += [0] * (6 - len(hourly))
    con.executemany("INSERT OR REPLACE INTO diag_counts VALUES(?,?,?)",
                    [(q["name"], k, n) for k, n in zip(keys, hourly)])
    con.commit()
    return hourly


PDIAG_VARIANTS = {  # name: (official API suffix, web-search suffix)
    "all": ("", ""),
    "noreply": (" -is:reply", " -filter:replies"),
    "reply": (" is:reply", " filter:replies"),
}


def pdiag_target(con, cfg, args, variant="all"):
    """Returns (query dict for this variant, web-search term, day, start). The variant gets its own
    query_name (e.g. regista:noreply) so counts and IDs are kept apart from the plain run."""
    qname = args[0] if args else cfg["queries"][0]["name"]
    q = next((x for x in cfg["queries"] if x["name"] == qname), None)
    if q is None:
        raise SystemExit(f"config.json に {qname} という検索条件がありません。")
    if len(args) < 2:
        raise SystemExit("日付を指定してください（例: py xc.py pdiag socialdata regista 2026-11-05）")
    if variant not in PDIAG_VARIANTS:
        raise SystemExit(f"不明な絞り込み: {variant}（{', '.join(PDIAG_VARIANTS)}, split）")
    o_suffix, w_suffix = PDIAG_VARIANTS[variant]
    qv = {**q, "query": q["query"] + o_suffix,
          "name": q["name"] if variant == "all" else f"{q['name']}:{variant}"}
    term = web_query(q["query"]) + w_suffix
    return qv, term, args[1], dt.datetime.strptime(args[1], "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)


def cmd_pdiag(env, con, cfg, args=()):
    """Keyword-search coverage of one provider: six 1-hour windows (UTC 0-6h) vs official hourly counts.
    Usage: py xc.py pdiag <socialdata|twitterapi_io|scrapebadger|apify|twex> <query> <day> [all|noreply|reply|split]
           py xc.py pdiag compare <query> <day> [all|noreply|reply]
           py xc.py pdiag who <query> <day>   (per-account or per-post? official posts with author IDs, ~$2)
    split = run noreply, then reply, and print how coverage differs between them."""
    pdiag_tables(con)
    if not args:
        raise SystemExit("使い方: py xc.py pdiag <サービス> <検索条件> <日付>  /  py xc.py pdiag compare <検索条件> <日付>\n"
                         f"サービス: {', '.join(list(PROVIDERS) + ['twex'])}")
    if args[0] == "compare":
        return pdiag_compare(con, cfg, args[1:3], args[3] if len(args) > 3 else "all")
    if args[0] == "who":
        return pdiag_who(env, con, cfg, args[1:3])
    pname = args[0]
    if pname == "twex":
        prov = PTwex(env, cfg, con)
    elif pname in PROVIDERS:
        prov = PROVIDERS[pname](env, cfg)
        prov.cfg_actor = cfg.get("apify_actor", "jacquemus~x-tweet-scraper")
    else:
        raise SystemExit(f"不明なサービス: {pname}（{', '.join(list(PROVIDERS) + ['twex'])}）")
    variant = args[3] if len(args) > 3 else "all"
    if variant == "split":
        res = {v: pdiag_run(env, con, cfg, prov, pname, args[1:3], v) for v in ("noreply", "reply")}
        print("返信の有無で分けた取得率:")
        for v, (got, n) in res.items():
            print(f"  {'返信以外' if v == 'noreply' else '返信'}: {got:,} / {n:,} 件（{got / max(1, n):.0%}）")
        return
    pdiag_run(env, con, cfg, prov, pname, args[1:3], variant)


def pdiag_run(env, con, cfg, prov, pname, targs, variant):
    q, term, day, start = pdiag_target(con, cfg, targs, variant)
    hourly = pdiag_hourly(env, con, cfg, q, start)
    print(f"[{prov.label}] [{q['name']}] {day} 0〜6時（UTC）の公式の件数: {sum(hourly):,}（1時間ごと {hourly}）")
    total = set()
    for h in range(6):
        s = start + dt.timedelta(hours=h)
        ws = iso(s.isoformat())
        ids, pages, why = prov.window(term, s, s + dt.timedelta(hours=1), hourly[h])
        con.execute("DELETE FROM diag_ids WHERE provider=? AND query_name=? AND window_start=?", (pname, q["name"], ws))
        con.executemany("INSERT OR IGNORE INTO diag_ids VALUES(?,?,?,?)", [(pname, q["name"], ws, i) for i in ids])
        con.execute("INSERT OR REPLACE INTO diag_runs VALUES(?,?,?,?,?,?,?)",
                    (pname, q["name"], ws, len(ids), pages, why, now()))
        con.commit()
        total |= ids
        print(f"    {h}時台: {len(ids):,} 件 / 公式 {hourly[h]:,} 件（{len(ids) / max(1, hourly[h]):.0%}、{pages} ページ、{why}）")
    n = sum(hourly)
    print(f"{prov.label}（1時間の枠6つ）: {len(total):,} / {n:,} 件（{len(total) / max(1, n):.0%}）  {prov.pace()}")
    return len(total), n


def pdiag_compare(con, cfg, args, variant="all"):
    q, _term, day, start = pdiag_target(con, cfg, args, variant)
    keys = [iso((start + dt.timedelta(hours=h)).isoformat()) for h in range(6)]
    ph = ",".join("?" * 6)
    official = con.execute(f"SELECT COALESCE(SUM(n),0), COUNT(*) FROM diag_counts WHERE query_name=? "
                           f"AND window_start IN ({ph})", (q["name"], *keys)).fetchone()
    if official[1] < 6:
        raise SystemExit("公式の件数がまだありません。先にどれかのサービスで pdiag を実行してください。")
    n = official[0]
    done = [r[0] for r in con.execute(f"SELECT provider FROM diag_runs WHERE query_name=? AND window_start IN ({ph}) "
                                      f"GROUP BY provider HAVING COUNT(*)=6 ORDER BY provider", (q["name"], *keys))]
    sets = {p: {r[0] for r in con.execute(f"SELECT post_id FROM diag_ids WHERE provider=? AND query_name=? "
                                          f"AND window_start IN ({ph})", (p, q["name"], *keys))} for p in done}
    if not sets:
        raise SystemExit("6つの枠すべてを終えたサービスがまだありません。")
    union = set().union(*sets.values())
    common = set.intersection(*sets.values())
    print(f"[{q['name']}] {day} 0〜6時（UTC） 公式の件数: {n:,}")
    print(f"{'サービス':<16}{'取得':>7}{'取得率':>8}{'他にない投稿':>12}")
    for p, s in sets.items():
        others = set().union(*(v for k, v in sets.items() if k != p)) if len(sets) > 1 else set()
        print(f"{p:<16}{len(s):>7,}{len(s) / max(1, n):>8.0%}{len(s - others):>12,}")
    print(f"全サービスの和集合: {len(union):,} 件（{len(union) / max(1, n):.0%}）")
    print(f"全サービス共通:     {len(common):,} 件")
    if len(sets) > 1:
        print("2社ずつの重なり（共通 / 和集合）:")
        names = list(sets)
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = sets[names[i]], sets[names[j]]
                print(f"  {names[i]} × {names[j]}: {len(a & b):,} / {len(a | b):,}（{len(a & b) / max(1, len(a | b)):.0%}）")
    print("判定（事前に決めたルール）: 80%以上 → 起点の収集をそのサービスに切り替える案を検討 / "
          "どのサービスも20%前後で同じ投稿 → X 側の除外と確定し ADR-0015 のまま / その間 → 相談")


def pdiag_who(env, con, cfg, args):
    """Is the ~20% visibility decided per account or per post? Fetch every official post in UTC 0-6h
    (IDs, author IDs and reply flag only; no text, about $0.005 per post), then look at each author:
    all visible / none visible / mixed. Under per-post sampling, mixed authors should be common;
    under per-account exclusion, they should be rare. Aggregates only (ADR-0004)."""
    q, _term, day, start = pdiag_target(con, cfg, args)
    keys = [iso((start + dt.timedelta(hours=h)).isoformat()) for h in range(6)]
    ph = ",".join("?" * 6)
    have = con.execute(f"SELECT COUNT(*) FROM diag_official WHERE query_name=? AND window_start IN ({ph})",
                       (q["name"], *keys)).fetchone()[0]
    if not have:
        hourly = pdiag_hourly(env, con, cfg, q, start)
        cl = Client(env["X_BEARER_TOKEN"], con, cfg)
        price = cl.prices.get("post", 0)
        print(f"公式APIで {sum(hourly):,} 件を取得します（推定 ${sum(hourly) * price:.2f}、本文は保存しません）")
        for k in keys:
            e = iso((dt.datetime.fromisoformat(k.replace("Z", "+00:00")) + dt.timedelta(hours=1)).isoformat())
            token = None
            while True:
                params = {"query": q["query"], "max_results": 100, "start_time": k, "end_time": e,
                          "tweet.fields": "author_id,referenced_tweets,in_reply_to_user_id"}
                if token:
                    params["next_token"] = token
                d = cl.get("/tweets/search/all", params, 100 * price)
                data = d.get("data", [])
                cl.charge("search/all(pdiag who)", "post", len(data))
                con.executemany("INSERT OR IGNORE INTO diag_official VALUES(?,?,?,?,?)", [
                    (q["name"], k, t["id"], t.get("author_id"),
                     int(any(r.get("type") == "replied_to" for r in t.get("referenced_tweets") or [])))
                    for t in data])
                con.commit()
                token = d.get("meta", {}).get("next_token")
                time.sleep(1.1)  # full-archive search: 1 request/sec
                if not token:
                    break
        print(f"公式API 推定使用額 ${cl.spent():.2f}")
    official = con.execute(f"SELECT post_id, author_id, is_reply FROM diag_official WHERE query_name=? "
                           f"AND window_start IN ({ph})", (q["name"], *keys)).fetchall()
    visible = {r[0] for r in con.execute(f"SELECT DISTINCT post_id FROM diag_ids WHERE query_name=? "
                                         f"AND window_start IN ({ph})", (q["name"], *keys))}
    if not visible:
        raise SystemExit("他社サービスの結果がまだありません。先に pdiag <サービス> を実行してください。")
    o_ids = {r[0] for r in official}
    p = len(visible & o_ids) / max(1, len(o_ids))
    print(f"[{q['name']}] {day} 0〜6時（UTC） 公式の投稿 {len(o_ids):,} 件 / 他社で見えた投稿（全社の和集合） "
          f"{len(visible & o_ids):,} 件（{p:.0%}）  公式に無い他社の投稿 {len(visible - o_ids):,} 件")
    by_author = {}
    for pid, aid, _r in official:
        a = by_author.setdefault(aid, [0, 0])
        a[0] += 1
        a[1] += pid in visible
    print(f"投稿者 {len(by_author):,} 人")
    print(f"{'投稿数':<10}{'人数':>6}{'全部見える':>10}{'全部見えない':>12}{'混在':>6}{'混在の期待値（投稿単位で無作為なら）':>22}")
    groups = [("1件", 1, 1), ("2件", 2, 2), ("3〜4件", 3, 4), ("5件以上", 5, 10 ** 9)]
    tot_mixed = tot_exp = 0.0
    for label, lo, hi in groups:
        g = [v for v in by_author.values() if lo <= v[0] <= hi]
        allv = sum(1 for n, s in g if s == n)
        none = sum(1 for n, s in g if s == 0)
        mixed = len(g) - allv - none
        exp = sum(1 - p ** n - (1 - p) ** n for n, _ in g) if lo >= 2 else 0
        if lo >= 2:
            tot_mixed += mixed
            tot_exp += exp
        exp_s = f"{exp:.1f}" if lo >= 2 else "-"
        print(f"{label:<10}{len(g):>6}{allv:>10}{none:>12}{mixed:>6}{exp_s:>22}")
    multi = [v for v in by_author.values() if v[0] >= 2]
    print(f"2件以上の投稿者 {len(multi)} 人: 混在 {tot_mixed:.0f} 人 / 投稿単位で無作為な場合の期待値 {tot_exp:.1f} 人")
    reply_n = sum(r[2] for r in official)
    reply_v = sum(1 for r in official if r[2] and r[0] in visible)
    print(f"参考: 返信 {reply_v}/{reply_n} 件見える、返信以外 {len(visible & o_ids) - reply_v}/{len(o_ids) - reply_n} 件見える")
    print("判定の目安: 混在が期待値に近い → 投稿単位の抽出（アカウントは関係ない） / "
          "混在がほぼ0で「全部見えない」人が多い → アカウント単位の除外 / その間 → 両方が混ざっている")


def cmd_sdiag(env, con, cfg, args=()):
    """Kept for compatibility: same as pdiag socialdata."""
    return cmd_pdiag(env, con, cfg, ("socialdata", *args))


def cmd_run(env, con, cfg):
    print("収集を始めます。Ctrl+C で中断しても、同じコマンドで続きから再開します。PCはつけたままにしてください。\n")
    if need_counts(con, cfg):
        cmd_count(env, con, cfg)
    cmd_search(env, con, cfg)
    cmd_sample(env, con, cfg)
    cmd_users(env, con, cfg)
    cmd_tbaseline(env, con, cfg)
    cmd_tweights(env, con, cfg)
    cmd_tany(env, con, cfg)
    cmd_export(env, con, cfg)
    print("\n完了しました。Claude に「終わった」と伝えてください。")


# ---------------------------------------------------------------- phase-1 rule (ADR-0011, ADR-0014)

BIO_RE_CACHE = {}


def phase1_class(cfg, created, bio, tot, day):
    """Pre-registered rule. Returns specialized / general / low_activity."""
    r = cfg["phase1_rule"]
    pat = BIO_RE_CACHE.setdefault("p", re.compile("|".join(r["bio_patterns"])))
    if created and iso(created) >= iso(r["new_account_after"]):
        return "specialized"
    if pat.search(bio or ""):
        return "specialized"
    if tot is None or tot < int(r["min_baseline_posts"]):
        return "low_activity"
    return "specialized" if (day or 0) / tot < float(r["daily_ratio_threshold"]) else "general"


# ---------------------------------------------------------------- pseudonymized export

def salt():
    if not os.path.exists(SALT_FILE):
        with open(SALT_FILE, "w", encoding="utf-8") as f:
            f.write(secrets.token_hex(32))
    with open(SALT_FILE, encoding="utf-8") as f:
        return bytes.fromhex(f.read().strip())


def cmd_export(env, con, cfg):
    s = salt()

    def pid(kind, x):
        if not x:
            return None
        return kind + hmac.new(s, f"{kind}:{x}".encode(), hashlib.sha256).hexdigest()[:16]

    handle_to_id = {(r[0] or "").lower(): r[1] for r in con.execute("SELECT username, id FROM users")}

    def mention_pid(m):
        if m.get("id"):
            return pid("u", m["id"])
        h = (m.get("username") or "").lower()
        return pid("u", handle_to_id[h]) if h in handle_to_id else pid("h", h)

    def scrub_post(text, mentions):
        text = URL_RE.sub("<URL>", text or "")
        for m in sorted(mentions, key=lambda m: -len(m.get("username") or "")):
            if m.get("username"):
                text = re.sub("@" + re.escape(m["username"]) + r"(?![A-Za-z0-9_])", "@" + mention_pid(m), text, flags=re.I)
        return re.sub(r"@(?![uh][0-9a-f]{16})[A-Za-z0-9_]{1,15}", "@<USER>", text)

    def scrub_bio(text):
        return AT_RE.sub("@<USER>", URL_RE.sub("<URL>", text or ""))

    daily_re = re.compile("|".join(re.escape(w) for w in cfg.get("daily_words", [])) or "(?!)")
    name_kw = cfg.get("name_keywords", [])
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)
    out = sqlite3.connect(OUT_DB)
    out.executescript("""
    CREATE TABLE accounts(pid TEXT PRIMARY KEY, created_at TEXT, followers INTEGER, following INTEGER,
        tweet_count INTEGER, listed INTEGER, protected INTEGER, verified INTEGER,
        bio TEXT, bio_mention_count INTEGER, bio_has_url INTEGER, has_url INTEGER, has_location INTEGER,
        default_image INTEGER, name_keyword_hits TEXT, seed_queries TEXT, seed_post_count INTEGER,
        stratum TEXT, in_sample INTEGER, baseline_window TEXT, baseline_n INTEGER, baseline_daily INTEGER,
        baseline_has_more INTEGER, pred_class TEXT, label_sample INTEGER, stratum_match_posts INTEGER,
        any_controversy_n INTEGER, any_controversy_more INTEGER);
    CREATE TABLE posts(pid TEXT PRIMARY KEY, author_pid TEXT, created_at TEXT, text TEXT, lang TEXT,
        reply_to_user_pid TEXT, ref_type TEXT, ref_post_pid TEXT, mention_pids TEXT,
        likes INTEGER, reposts INTEGER, replies INTEGER, quotes INTEGER,
        is_seed INTEGER, seed_queries TEXT, in_sample INTEGER, source TEXT);
    CREATE TABLE daily_counts(query_name TEXT, day TEXT, official_n INTEGER, seeds_n INTEGER);
    CREATE TABLE audit(post_pid TEXT, query_name TEXT, window_start TEXT, in_twex INTEGER,
        is_reply INTEGER, is_quote INTEGER, n_mentions INTEGER, text TEXT);
    CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
    """)
    sample = dict(con.execute("SELECT user_id, query_name FROM sample"))
    seedq = {}
    for post_id, qn in con.execute("SELECT post_id, query_name FROM seed_hits"):
        seedq.setdefault(post_id, []).append(qn)
    acct_seed, acct_seed_n = {}, {}
    for p_id, aid in con.execute("SELECT id, author_id FROM posts"):
        if p_id in seedq:
            acct_seed.setdefault(aid, set()).update(seedq[p_id])
            acct_seed_n[aid] = acct_seed_n.get(aid, 0) + 1
    base = {r[0]: r[1:] for r in con.execute("SELECT user_id, window, n, has_more FROM baseline_state")}
    bdaily = {}
    for aid, text in con.execute("SELECT author_id, text FROM posts WHERE (','||source||',') LIKE '%,baseline,%' "
                                 "OR (','||source||',') LIKE '%,recent,%'"):
        bdaily[aid] = bdaily.get(aid, 0) + int(bool(daily_re.search(text or "")))
    probe = {r[0]: r[1:] for r in con.execute("SELECT user_id, match_n, any_n, any_more FROM probe")}

    # stratified label sample for Claude's automatic labeling of account type (ADR-0012, ADR-0014)
    rng = random.Random(int(cfg.get("random_seed", 42)) + 1)
    per_g = int(cfg.get("label_sample_per_group", 25))
    users = con.execute("SELECT * FROM users").fetchall()
    pc_of = {}
    for u in users:
        uid, created, desc = u[0], u[4], u[3]
        b = base.get(uid)
        if uid in sample and b and b[0] == "baseline":
            pc_of[uid] = phase1_class(cfg, created, desc, b[1], bdaily.get(uid, 0))
        elif uid in sample and b:
            pc_of[uid] = phase1_class(cfg, created, desc, None, None)
    label = set()
    for q in cfg["queries"]:
        by_c = {}
        for uid, g in sample.items():
            if g == q["name"] and uid in pc_of:
                by_c.setdefault(pc_of[uid], []).append(uid)
        for v in by_c.values():
            v.sort()
            rng.shuffle(v)
        classes, take = sorted(by_c), []
        while len(take) < per_g and any(by_c[c] for c in classes):
            for c in classes:
                if by_c[c] and len(take) < per_g:
                    take.append(by_c[c].pop())
        label.update(take)

    for u in users:
        (uid, _uname, name, desc, created, fol, fng, tc, listed, prot, ver, _vt, url, loc, _pin, img, _f) = u
        b = base.get(uid, (None, None, None))
        pr = probe.get(uid, (None, None, None))
        out.execute("INSERT INTO accounts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            pid("u", uid), created, fol, fng, tc, listed, prot, ver,
            scrub_bio(desc), len(AT_RE.findall(desc or "")), int(bool(URL_RE.search(desc or ""))),
            int(bool(url)), int(bool(loc)), int("default_profile" in (img or "")),
            json.dumps([k for k in name_kw if k and k in (name or "")], ensure_ascii=False),
            json.dumps(sorted(acct_seed.get(uid, [])), ensure_ascii=False), acct_seed_n.get(uid, 0),
            sample.get(uid), int(uid in sample), b[0], b[1], bdaily.get(uid) if b[0] else None, b[2],
            pc_of.get(uid), int(uid in label), pr[0], pr[1], pr[2]))

    for row in con.execute("SELECT id, author_id, created_at, text, lang, in_reply_to_user_id, referenced, "
                           "mentions, metrics, source FROM posts"):
        (i, aid, created, text, lang, rto, refs, ments, mets, src_) = row
        refs, ments, mets = json.loads(refs or "[]"), json.loads(ments or "[]"), json.loads(mets or "{}")
        ref = refs[0] if refs else {}
        out.execute("INSERT INTO posts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            pid("p", i), pid("u", aid), created, scrub_post(text, ments), lang,
            pid("u", rto), ref.get("type"), pid("p", ref.get("id")),
            json.dumps([mention_pid(m) for m in ments if m.get("username") or m.get("id")]),
            mets.get("like_count"), mets.get("retweet_count"), mets.get("reply_count"), mets.get("quote_count"),
            int(i in seedq), json.dumps(sorted(seedq.get(i, [])), ensure_ascii=False), int(aid in sample), src_))

    twex_daily = {(qn, st[:10]): n for qn, st, n in con.execute("SELECT query_name, start, n FROM osearch_state")}
    out.executemany("INSERT INTO daily_counts VALUES(?,?,?,?)",
                    [(qn, st[:10], n, twex_daily.get((qn, st[:10])))
                     for qn, st, n in con.execute("SELECT query_name, start, n FROM daily_counts")])
    for r in con.execute("SELECT post_id, query_name, window_start, in_twex, is_reply, is_quote, n_mentions, text "
                         "FROM audit"):
        out.execute("INSERT INTO audit VALUES(?,?,?,?,?,?,?,?)",
                    (pid("p", r[0]), r[1], r[2], r[3], r[4], r[5], r[6], scrub_post(r[7], [])))

    out.executemany("INSERT INTO meta VALUES(?,?)", [
        ("exported_at", now()),
        ("queries", json.dumps(cfg["queries"], ensure_ascii=False)),
        ("start_time", cfg.get("start_time") or ""), ("end_time", cfg.get("end_time") or ""),
        ("random_seed", str(cfg.get("random_seed", 42))),
        ("design", json.dumps({k: cfg.get(k) for k in (
            "baseline_start", "baseline_end", "baseline_max_posts", "recent_max_posts", "daily_words", "phase1_rule",
            "label_sample_per_group", "any_controversy_query", "any_controversy_groups", "twex_time_format")},
            ensure_ascii=False)),
        ("n_seed_authors", json.dumps({q["name"]: con.execute(
            "SELECT COUNT(DISTINCT p.author_id) FROM posts p JOIN seed_hits s ON s.post_id=p.id WHERE s.query_name=?",
            (q["name"],)).fetchone()[0] for q in cfg["queries"]})),
    ])
    out.commit()
    a = out.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
    p = out.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    out.close()
    print(f"analysis.sqlite を作成しました（アカウント {a:,} 件・投稿 {p:,} 件、ID・@名・URLは仮名化済み）。")


# ---------------------------------------------------------------- housekeeping

def cmd_purge(yes):
    if not yes:
        print("raw.sqlite と analysis.sqlite を削除します。実行するには: py xc.py purge --yes")
        return
    for p in (RAW_DB, OUT_DB):
        if os.path.exists(p):
            os.remove(p)
            print("削除:", os.path.basename(p))


def cmd_status(env, con, cfg):
    q = lambda s: con.execute(s).fetchone()[0]
    print(f"起点の投稿:           {q('SELECT COUNT(DISTINCT post_id) FROM seed_hits'):,}")
    print(f"抽出したアカウント:   {q('SELECT COUNT(*) FROM sample'):,}")
    print(f"プロフィール取得済:   {q('SELECT COUNT(*) FROM sample s JOIN users u ON u.id=s.user_id WHERE u.username IS NOT NULL'):,}")
    print(f"ふだんの投稿を取得済: {q('SELECT COUNT(*) FROM baseline_state'):,}")
    print(f"重み用の件数を取得済: {q('SELECT COUNT(*) FROM probe WHERE match_n IS NOT NULL'):,}")
    print(f"保存中の投稿（合計）: {q('SELECT COUNT(*) FROM posts'):,}")
    print(f"TwexAPI クレジット:   {q('SELECT COALESCE(SUM(credits),0) FROM twex_ledger'):,}")
    print(f"公式API 推定使用額:   ${q('SELECT COALESCE(SUM(usd),0) FROM ledger'):.2f}")


COMMANDS = {"tcheck": cmd_tcheck, "tdiag": cmd_tdiag, "sdiag": cmd_sdiag, "pdiag": cmd_pdiag, "check": cmd_check, "run": cmd_run, "count": cmd_count,
            "search": cmd_search, "sample": cmd_sample, "users": cmd_users, "tbaseline": cmd_tbaseline,
            "tweights": cmd_tweights, "tany": cmd_tany, "export": cmd_export, "status": cmd_status}


class Tee:
    """Write everything printed to the screen into a log file as well (logs/<command>_<time>.log)."""

    def __init__(self, stream, f):
        self.stream, self.f = stream, f

    def write(self, s):
        self.stream.write(s)
        self.f.write(s)
        self.f.flush()

    def flush(self):
        self.stream.flush()
        self.f.flush()


def start_log(words):
    os.makedirs(LOG_DIR, exist_ok=True)
    tag = "_".join(re.sub(r"[^\w.-]", "", w) for w in words)[:80]
    path = os.path.join(LOG_DIR, f"{dt.datetime.now():%Y%m%d_%H%M%S}_{tag}.log")
    f = open(path, "w", encoding="utf-8")
    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=list(COMMANDS) + ["purge"])
    ap.add_argument("args", nargs="*")
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    if a.command == "purge":
        return cmd_purge(a.yes)
    log_path = start_log([a.command, *a.args])
    print(f"# {now()}  py xc.py {' '.join([a.command, *a.args])}")
    cfg, con, env = load_config(), raw_db(), load_env()
    try:
        if a.command in ("tdiag", "sdiag", "pdiag"):
            COMMANDS[a.command](env, con, cfg, a.args)
        else:
            COMMANDS[a.command](env, con, cfg)
    except BudgetExceeded as e:
        print(e)
    except KeyboardInterrupt:
        print("\n中断しました。同じコマンドを再実行すると続きから再開します。")
    except SystemExit as e:
        if e.code not in (None, 0):
            print(e.code)
        raise SystemExit(1 if e.code not in (None, 0) else 0)
    finally:
        con.commit()
        print(f"# 終了 {now()}  ログ: {log_path}")


if __name__ == "__main__":
    main()
