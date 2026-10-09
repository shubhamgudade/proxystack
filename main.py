
"""
ProxyStack v2 — Simplified Distributed Proxy Gateway
=====================================================
One file. Multiple machines. Change ROLE env var, different behavior.

ROLES:
  checker_1  through checker_10 → fetch from source slice, check meesho, push to pool
  pool       → Omen  — ingest, pick, dead, recheck, persist, dashboard SSE
  dashboard  → Yoru  — stats aggregator + dashboard UI
  vps        → Oracle — /request, pinger, paid proxies, main app API

Pipeline (per checker):
  Boot → fetch latest 500 from my sources → check meesho → push live → pool
  Loop:
    Poll repos every 60s for new commits
      new commit  → fetch up to 1000 proxies → check → push pool
      no commit   → grab next 500 from known list (pointer advances) → check → push pool
  Dedup: per-instance seen set, 1hr TTL — never recheck same addr in cycle

Pool:
  /ingest    → store with timestamp + latency label
  /pick      → serve freshest proxy (flash/panther preferred)
  /dead      → immediate evict
  recheck    → every 8min, proxies older than 5min → meesho check → evict if dead
  hard TTL   → 15min evict regardless
  dashboard  → served here, SSE /stream/stats
"""

import asyncio
import gzip
import json
import os
import random
import re
import socket
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import requests
import urllib3
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

urllib3.disable_warnings()

# ═══════════════════════════════════════════════════════════════════════════════
# ROLE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
ROLE          = os.getenv("ROLE", "pool").lower()
SHARED_SECRET = os.getenv("SHARED_SECRET", "changeme")
PORT          = int(os.getenv("PORT", "8080"))
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "")

# ─── Peer URLs ────────────────────────────────────────────────────────────────
PEERS = {
    "checker_1":  os.getenv("PEER_CHECKER_1",  "https://ps-brimstone.onrender.com"),
    "checker_2":  os.getenv("PEER_CHECKER_2",  "https://ps-viper.onrender.com"),
    "checker_3":  os.getenv("PEER_CHECKER_3",  "https://ps-sage.onrender.com"),
    "checker_4":  os.getenv("PEER_CHECKER_4",  "https://ps-sova.onrender.com"),
    "checker_5":  os.getenv("PEER_CHECKER_5",  "https://ps-astra.onrender.com"),
    "checker_6":  os.getenv("PEER_CHECKER_6",  "https://ps-harbor.onrender.com"),
    "checker_7":  os.getenv("PEER_CHECKER_7",  "https://ps-phoenix.onrender.com"),
    "checker_8":  os.getenv("PEER_CHECKER_8",  "https://ps-breach.onrender.com"),
    "checker_9":  os.getenv("PEER_CHECKER_9",  "https://ps-neon.onrender.com"),
    "checker_10": os.getenv("PEER_CHECKER_10", "https://ps-killjoy.onrender.com"),
    "pool":       os.getenv("PEER_POOL",       "https://ps-omen.onrender.com"),
    "dashboard":  os.getenv("PEER_DASHBOARD",  "https://ps-yoru.onrender.com"),
    "vps":        os.getenv("PEER_VPS",        "http://130.210.16.206:8080"),
}

CHECKER_ROLES = [f"checker_{i}" for i in range(1, 11)]

# ═══════════════════════════════════════════════════════════════════════════════
# CALLER IDENTIFICATION — port → label, logged only, no blocking
# ═══════════════════════════════════════════════════════════════════════════════
CALLER_PORT_MAP: dict[int, str] = {
    8001: "MesoWebBackend",
    8090: "pinger_dashboard",
    8080: "proxystack-internal",
}

def _identify_caller(request: Request) -> str:
    if request.client is None:
        return "unknown"
    port = request.client.port
    return CALLER_PORT_MAP.get(port, f"unknown:{port}")

# ═══════════════════════════════════════════════════════════════════════════════
# PATHS
# ═══════════════════════════════════════════════════════════════════════════════
BASE         = Path(__file__).parent
PERSIST_FREE = BASE / "proxy_live.json"
PERSIST_PAID = BASE / "paid_live.json"
KEYS_FILE    = BASE / "keys.json"

# ═══════════════════════════════════════════════════════════════════════════════
# TUNING
# ═══════════════════════════════════════════════════════════════════════════════
BOOT_FETCH_COUNT    = 500      # proxies to fetch on boot
COMMIT_FETCH_COUNT  = 1000     # proxies to fetch per new commit
CYCLE_FETCH_COUNT   = 500      # proxies per normal cycle
COMMIT_POLL_S       = 60       # how often to poll for new commits
CHECK_CONCURRENCY   = 150      # concurrent meesho checks per checker
CHECK_TIMEOUT_S     = 12       # meesho check timeout
FETCH_WORKERS       = 20       # concurrent source fetchers
SEEN_TTL_S          = 3600     # 1hr — don't recheck same proxy in cycle
INGEST_BATCH_SIZE   = 100
INGEST_INTERVAL_S   = 1.0

# pool tuning
POOL_RECHECK_INTERVAL_S = 480
POOL_RECHECK_MIN_AGE_S  = 300
POOL_RECHECK_BATCH      = 80
POOL_TTL_S              = 900
POOL_COOLDOWN_S         = 8

# request freshness tuning
PICK_WAIT_S          = 0.0
PICK_FAST_MAX_S      = 4.0
PICK_FALLBACK_COUNT  = 10

SNAP_INTERVAL_S         = 2
PERSIST_INTERVAL_S      = 60
# latency categories
CAT_FLASH   = 3.0
CAT_PANTHER = 5.0
CAT_LANTERN = 7.0
CAT_DEAD    = 10.0

FRESH_HOT_S  = 30
FRESH_COLD_S = 120

# paid
MEESHO_API  = "https://prod.meeshoapi.com"
MEESHO_AUTH = "32c4d8137cn9eb493a1921f203173080"
APP_ID      = "com.meesho.supply"

STICKY_TTL_S = 90
PING_INTERVAL = 240

FOD_HUNT_PATHS = (
    "/api/1.0/anonymous/config",
    "/api/1.0/anonymous/referral-app-install",
    "/api/1.0/anonymous/fod-personalisation",
)


# ═══════════════════════════════════════════════════════════════════════════════
# SERVER-SIDE REQUEST RACING
# ═══════════════════════════════════════════════════════════════════════════════

RACE_WAIT_S      = 2.0
RACE_FRESH_MAX_S = 4.0
RACE_PROXY_COUNT = 10

STICKY_URL_FRAGMENTS = [
    "/api/1.0/cart",
    "/api/8.0/cart",
    "/api/1.0/cart/add",
    "/api/1.0/cart/location",
    "/api/1.0/cart/paymentinfo",
    "/api/3.0/order",
    "/api/4.0/preorders",
    "/api/2.0/orders",
    "/api/3.0/user/orders",
    "/api/3.0/user/order-details",
    "/api/3.0/addresses",
    "/api/2.0/addresses",
    "/api/1.0/user/delivery-location",
    "/api/1.0/suborders/ratings/pending",
]

RACEABLE_URL_FRAGMENTS = [
    "/api/3.0/search/suggest",
    "/api/3.0/anonymous/search/suggest",
    "/api/1.0/widget-groups/fetch",
    "/api/1.0/anonymous/widget-groups/fetch",
    "/api/3.0/product/static",
    "/api/3.0/product/dynamic",
    "/api/2.0/catalogs/",
    "/api/2.0/anonymous/catalogs/",
    "/api/1.0/catalogs/recommendations",
    "/api/1.0/anonymous/catalogs/recommendations",
    "/api/4.0/anonymous/for-you",
    "/api/3.0/user/orders",
    "/api/3.0/user/order-details",
    "/api/1.0/anonymous/fod-personalisation",
    "/api/1.0/anonymous/config",
]

def _is_sticky_url(url: str) -> bool:
    return any(frag in url for frag in STICKY_URL_FRAGMENTS)

def _is_raceable_url(url: str) -> bool:
    # Sticky wins if a URL appears in both lists.
    if _is_sticky_url(url):
        return False
    return any(frag in url for frag in RACEABLE_URL_FRAGMENTS)

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCES
# ═══════════════════════════════════════════════════════════════════════════════
ALL_GITHUB_REPOS = [
    ("monosans",           "proxy-list",          "proxies/http.txt"),
    ("monosans",           "proxy-list",          "proxies_anonymous/http.txt"),
    ("TheSpeedX",          "PROXY-List",          "http.txt"),
    ("proxifly",           "free-proxy-list",     "proxies/protocols/http/data.txt"),
    ("proxifly",           "free-proxy-list",     "proxies/all/data.txt"),
    ("roosterkid",         "openproxylist",       "HTTPS_RAW.txt"),
    ("mmpx12",             "proxy-list",          "http.txt"),
    ("clarketm",           "proxy-list",          "proxy-list-raw.txt"),
    ("ShiftyTR",           "Proxy-List",          "http.txt"),
    ("rdavydov",           "proxy-list",          "proxies/http.txt"),
    ("rdavydov",           "proxy-list",          "proxies_anonymous/http.txt"),
    ("zevtyardt",          "proxy-list",          "http.txt"),
    ("proxy4parsing",      "proxy-list",          "http.txt"),
    ("sunny9577",          "proxy-scraper",       "proxies.txt"),
    ("zloi-user",          "hideip.me",           "http.txt"),
    ("bq2015",             "FreeProxies",         "http.txt"),
    ("Ian-Lusule",         "Proxies",             "http.txt"),
    ("fate0",              "proxylist",           "proxy.list"),
    ("jetkai",             "proxy-list",          "online-proxies/txt/proxies-http.txt"),
    ("HyperBeats",         "Free-Proxies",        "http.txt"),
    ("elliottophellia",    "proxylist",           "http/HTTP_ALL.txt"),
    ("prxchk",             "proxy-list",          "http.txt"),
    ("Anonym0usWork1221",  "free-proxy-list",     "free-proxy/http.txt"),
    ("officialputuid",     "open-proxy-scrapper", "lists/http.txt"),
    ("ErcinDedeoglu",      "proxylists",          "lists/http.txt"),
    ("ObcbO",              "free-proxy",          "http.txt"),
    ("Vann-Dev",           "free-proxy-list",     "proxy-list/http.txt"),
    ("almroot",            "proxylist",           "list.txt"),
    ("aslisk",             "public_socks5_lists", "http.txt"),
    ("MuRongPIG",          "Free-Proxies",        "TheBiggerPicture.txt"),
    ("casals",             "hma-proxy-list",      "http.txt"),
    ("BlackSnowDot",       "proxy-list",          "http-proxies.txt"),
    ("im-razvan",          "http-proxy",          "http.txt"),
    ("UserR00T",           "Proxy-List",          "Proxylists/http.txt"),
    ("UptimerBot",         "proxy-list",          "proxies/http.txt"),
    ("yuceltoluyag",       "socks4-socks5-http-proxylist", "http.txt"),
    ("TundzhaSarl",        "free-proxy-list",     "lists/http.txt"),
    ("mertguvencli",       "http-proxy-list",     "list.txt"),
    ("proxylist2022",      "proxy-list",          "http.txt"),
    ("saisuiu",            "free-public-proxy",   "http.txt"),
    ("Zaeem20",            "free-proxies-scraper","Proxies/Http/http-proxies.txt"),
    ("roma8ka",            "free-proxy-list",     "http.txt"),
    ("MathiasMillingdale", "free-http-proxy-list","http.txt"),
    ("Fkunn1326",          "opz-proxies",         "http.txt"),
    ("hookzof",            "socks5_list",         "proxy/socks5.txt"),
    ("calpt",              "glitch-socks",        "http.txt"),
    ("iplocate",           "free-proxy-list",     "protocols/http.txt"),
    ("adasd223",           "checked-proxy-list",  "http.txt"),
    ("gfpcom",             "free-proxy-list",     "lists/http.txt"),
]

ALL_HTTP_SOURCES = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&anonymity=elite",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&anonymity=anonymous",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://www.proxy-list.download/api/v1/get?type=https",
    "https://api.openproxylist.xyz/http.txt",
    "https://proxyspace.pro/http.txt",
    "https://proxyspace.pro/https.txt",
    "https://proxyscan.io/download?type=http",
    "https://spys.me/proxy.txt",
    "https://multiproxy.org/txt_all/proxy.txt",
    "https://www.freeproxychecker.com/result/http_proxies.txt",
    "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/http/data.txt",
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
    "https://sockslist.us/Api?request=display&country=all&level=all&token=free",
    "http://pubproxy.com/api/proxy?limit=20&format=txt&type=http",
]

GEONODE_PAGES = [
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=http",
]

HTML_SOURCES = [
    "https://free-proxy-list.net",
    "https://sslproxies.org",
    "https://us-proxy.org",
    "https://free-proxy-list.net/anonymous-proxy.html",
]

def _split_sources(lst: list, n: int, idx: int) -> list:
    chunk = len(lst) // n
    start = idx * chunk
    end   = start + chunk if idx < n - 1 else len(lst)
    return lst[start:end]

def _get_my_sources():
    idx = int(ROLE.split("_")[1]) - 1
    n   = 10
    return (
        _split_sources(ALL_GITHUB_REPOS, n, idx),
        _split_sources(ALL_HTTP_SOURCES, n, idx),
        _split_sources(GEONODE_PAGES,   n, idx),
        _split_sources(HTML_SOURCES,    n, idx),
    )

# ═══════════════════════════════════════════════════════════════════════════════
# SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════
_IP_PORT = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b")

def _parse(text: str) -> list[str]:
    return [m.group(0) for m in _IP_PORT.finditer(text)]

_proxies_lock = threading.Lock()
_proxies: dict[str, dict] = {}

_used_lock  = threading.Lock()
_last_used: dict[str, float] = {}

_snap_lock     = threading.Lock()
_snap_fresh30:  list[str] = []
_snap_fresh120: list[str] = []
_snap_fast:     list[str] = []

_paid_lock    = threading.Lock()
_paid_proxies: dict[str, dict] = {}

_keys_lock = threading.Lock()
_keys: list[dict] = []

_seen_lock = threading.Lock()
_seen: dict[str, float] = {}

_outbuf_lock = threading.Lock()
_outbuf: list[tuple[str, float]] = []

# ─── Counters ─────────────────────────────────────────────────────────────────
_counter_lock = threading.Lock()
_counters_lifetime: dict[str, int] = defaultdict(int)
_counters_cycle:    dict[str, int] = defaultdict(int)
_cycle_start: float = time.time()

_activity_lock = threading.Lock()
_activity_log: list[dict] = []
ACTIVITY_MAX = 200

_persist_event = threading.Event()

_sha_lock  = threading.Lock()
_repo_sha: dict[str, str] = {}

_etag_lock = threading.Lock()
_http_etag: dict[str, str] = {}
_http_lmod: dict[str, str] = {}

_check_queue: asyncio.Queue = None
_check_loop:  asyncio.AbstractEventLoop = None

_sticky_lock = threading.Lock()
_sticky: dict[str, tuple] = {}

_api_executor     = ThreadPoolExecutor(max_workers=8,  thread_name_prefix="api")
_backend_executor = ThreadPoolExecutor(max_workers=24, thread_name_prefix="backend")

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _inc(key: str, n: int = 1):
    with _counter_lock:
        _counters_lifetime[key] += n
        _counters_cycle[key]    += n

def _reset_cycle():
    global _cycle_start
    with _counter_lock:
        _counters_cycle.clear()
        _cycle_start = time.time()

def _log(event: str, detail: str = "", count: int = 0):
    entry = {"ts": time.time(), "role": ROLE, "event": event, "detail": detail, "count": count}
    with _activity_lock:
        _activity_log.append(entry)
        if len(_activity_log) > ACTIVITY_MAX:
            del _activity_log[:-ACTIVITY_MAX]

def _assign_cat(avg_s: float) -> Optional[str]:
    if avg_s < CAT_FLASH:   return "flash"
    if avg_s < CAT_PANTHER: return "panther"
    if avg_s < CAT_LANTERN: return "lantern"
    if avg_s < CAT_DEAD:    return "deadass"
    return None

def _meesho_headers() -> dict:
    return {
        "authorization":       MEESHO_AUTH,
        "app-version":         "29.3",
        "app-version-code":    "864",
        "app-client-id":       "android",
        "app-sdk-version":     "36",
        "application-id":      APP_ID,
        "country-iso":         "in",
        "instance-id":         uuid.uuid4().hex,
        "app-session-id":      str(uuid.uuid4()),
        "app-session-count":   "1",
        "app-gaid":            str(uuid.uuid4()),
        "shield-session-id":   str(uuid.uuid4()),
        "meesho-user-context": "anonymous",
        "content-type":        "application/json; charset=UTF-8",
        "user-agent":          "okhttp/4.9.0",
        "accept-encoding":     "gzip",
    }

def _decode_meesho(raw: bytes, enc: str) -> dict:
    try:
        if "gzip"    in enc: raw = gzip.decompress(raw)
        elif "deflate" in enc: raw = zlib.decompress(raw)
    except Exception:
        pass
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _seen_dup(addr: str) -> bool:
    now = time.time()
    with _seen_lock:
        t = _seen.get(addr)
        if t is not None and now - t < SEEN_TTL_S:
            return True
        _seen[addr] = now
        if len(_seen) > 50_000:
            for k, v in list(_seen.items()):
                if now - v >= SEEN_TTL_S:
                    _seen.pop(k, None)
        return False

def _gh_headers() -> dict:
    h = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def _is_fod_hunt(url: str) -> bool:
    from urllib.parse import urlsplit
    try:
        path = urlsplit(url).path
    except Exception:
        return False
    return any(path.startswith(p) for p in FOD_HUNT_PATHS)

def _sticky_key(headers) -> str:
    if not headers:
        return ""
    low = {str(k).lower(): v for k, v in headers.items()}
    k = low.get("app-session-id") or low.get("instance-id")
    return f"fod:{k}" if k else ""

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE FETCHERS
# ═══════════════════════════════════════════════════════════════════════════════
def _fetch_github_raw(owner: str, repo: str, path: str, sha: str = "main") -> list[str]:
    try:
        r = requests.get(
            f"https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{path}",
            headers=_gh_headers(), timeout=10,
        )
        if r.status_code == 200:
            return _parse(r.text)
    except Exception as e:
        print(f"[gh] {owner}/{repo} raw fetch failed: {e}", flush=True)
    return []

def _fetch_github_latest(owner: str, repo: str, path: str) -> tuple[list[str], str]:
    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/commits",
            params={"path": path, "per_page": 1},
            headers=_gh_headers(), timeout=8,
        )
        if r.status_code in (403, 429):
            _inc("gh_ratelimited")
            return [], ""
        if r.status_code != 200:
            return [], ""
        commits = r.json()
        if not isinstance(commits, list) or not commits:
            return [], ""
        sha = commits[0].get("sha", "")
        if not sha:
            return [], ""
    except Exception:
        return [], ""
    proxies = _fetch_github_raw(owner, repo, path, sha)
    return proxies, sha

def _fetch_github_commit(owner: str, repo: str, path: str) -> tuple[list[str], str]:
    key = f"{owner}/{repo}/{path}"
    proxies, sha = _fetch_github_latest(owner, repo, path)
    if not sha:
        return [], ""
    with _sha_lock:
        old_sha = _repo_sha.get(key, "")
        if sha == old_sha:
            return [], ""
        _repo_sha[key] = sha
    return proxies[:COMMIT_FETCH_COUNT], sha

def _fetch_http_url(url: str) -> list[str]:
    hdrs = {}
    with _etag_lock:
        if url in _http_etag: hdrs["If-None-Match"]     = _http_etag[url]
        if url in _http_lmod: hdrs["If-Modified-Since"] = _http_lmod[url]
    try:
        r = requests.get(url, headers=hdrs, timeout=10)
        if r.status_code == 304:
            return []
        with _etag_lock:
            if "ETag"          in r.headers: _http_etag[url] = r.headers["ETag"]
            if "Last-Modified" in r.headers: _http_lmod[url] = r.headers["Last-Modified"]
        return _parse(r.text)
    except Exception:
        return []

def _fetch_geonode_page(url: str) -> list[str]:
    try:
        r    = requests.get(url, timeout=10)
        data = r.json()
        return [f"{p['ip']}:{p['port']}" for p in data.get("data", [])]
    except Exception:
        return []

def _fetch_html_page(url: str) -> list[str]:
    try:
        r    = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        out  = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    ip   = cols[0].get_text(strip=True)
                    port = cols[1].get_text(strip=True)
                    if re.match(r"\d{1,3}(?:\.\d{1,3}){3}", ip) and port.isdigit():
                        out.append(f"{ip}:{port}")
        return out
    except Exception:
        return []

def _fetch_checkerproxy() -> list[str]:
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r     = requests.get(f"https://checkerproxy.net/api/archive/{today}", timeout=10)
        data  = r.json()
        return [
            p["addr"] for p in data
            if p.get("addr") and re.match(r"\d+\.\d+\.\d+\.\d+:\d+", p["addr"])
        ]
    except Exception:
        return []

def _bulk_fetch(repos: list, http_srcs: list, geonode: list, html: list, limit: int) -> list[str]:
    collected: set[str] = set()
    results: list[str]  = []

    def _add(proxies: list[str]):
        for p in proxies:
            if p not in collected:
                collected.add(p)
                results.append(p)

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {}
        for o, r, p in repos:
            futs[ex.submit(_fetch_github_raw, o, r, p)] = f"gh:{o}/{r}"
        for url in http_srcs:
            futs[ex.submit(_fetch_http_url, url)] = f"http:{url[:40]}"
        for url in geonode:
            futs[ex.submit(_fetch_geonode_page, url)] = f"geo:{url[:40]}"
        for url in html:
            futs[ex.submit(_fetch_html_page, url)] = f"html:{url[:40]}"
        futs[ex.submit(_fetch_checkerproxy)] = "checkerproxy"

        for fut in as_completed(futs):
            try:
                _add(fut.result() or [])
            except Exception:
                pass
            if len(results) >= limit:
                break

    return results[:limit]

# ═══════════════════════════════════════════════════════════════════════════════
# MEESHO CHECK
# ═══════════════════════════════════════════════════════════════════════════════
async def _meesho_check(session: aiohttp.ClientSession, addr: str) -> tuple[bool, float]:
    hdr = _meesho_headers()
    t0  = asyncio.get_event_loop().time()
    try:
        async with session.get(
            f"{MEESHO_API}/api/1.0/anonymous/config",
            headers=hdr,
            proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT_S),
            ssl=False,
        ) as r:
            if r.status == 200:
                raw  = await r.read()
                enc  = r.headers.get("Content-Encoding", "")
                data = _decode_meesho(raw, enc)
                xoox = data.get("xoox", {})
                if isinstance(xoox, str):
                    try:    xoox = json.loads(xoox)
                    except: xoox = {}
                elapsed = asyncio.get_event_loop().time() - t0
                if isinstance(xoox, dict) and xoox.get("xo"):
                    return True, elapsed
    except Exception:
        pass
    return False, 0.0

# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOUND SENDER (checker → pool)
# ═══════════════════════════════════════════════════════════════════════════════
def _push_to_pool(addr: str, latency_s: float):
    with _outbuf_lock:
        _outbuf.append((addr, latency_s))

def _outbuf_sender():
    pool_url = PEERS.get("pool", "")
    while True:
        with _outbuf_lock:
            pending = len(_outbuf)
        sleep_s = 0.1 if pending > 50 else INGEST_INTERVAL_S
        time.sleep(sleep_s)
        with _outbuf_lock:
            if not _outbuf:
                continue
            batch = list(_outbuf[:INGEST_BATCH_SIZE])
            del _outbuf[:INGEST_BATCH_SIZE]
        if not pool_url:
            continue
        try:
            payload = [{"addr": a, "latency_s": l} for a, l in batch]
            r = requests.post(
                f"{pool_url}/ingest",
                json={"proxies": payload},
                headers={"X-Secret": SHARED_SECRET},
                timeout=10,
            )
            print(f"[{ROLE}] pushed {len(batch)} → pool ({r.status_code})", flush=True)
        except Exception as e:
            print(f"[{ROLE}] push failed: {e}", flush=True)
            with _outbuf_lock:
                for item in batch:
                    _outbuf.insert(0, item)

# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC CHECK PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
async def _checker_pipeline():
    global _check_queue
    _check_queue = asyncio.Queue(maxsize=10_000)
    sem          = asyncio.Semaphore(CHECK_CONCURRENCY)
    connector    = aiohttp.TCPConnector(
        limit=CHECK_CONCURRENCY + 50,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async def _worker(session: aiohttp.ClientSession):
        while True:
            addr = await _check_queue.get()
            async with sem:
                passed, latency = await _meesho_check(session, addr)
            if passed:
                cat = _assign_cat(latency)
                if cat:
                    _inc(f"check_pass_{cat}")
                    _log("check_pass", f"{addr} {cat} {latency:.1f}s")
                    _push_to_pool(addr, latency)
                else:
                    _inc("check_too_slow")
            else:
                _inc("check_fail")

    async def _stats_printer():
        while True:
            await asyncio.sleep(15)
            q = _check_queue.qsize() if _check_queue else 0
            with _outbuf_lock: ob = len(_outbuf)
            with _counter_lock:
                life  = dict(_counters_lifetime)
                cycle = dict(_counters_cycle)
            cats = ('flash', 'panther', 'lantern', 'deadass')
            print(
                f"[{ROLE}] q={q} outbuf={ob} | "
                f"CYCLE  pass={sum(cycle.get(f'check_pass_{c}', 0) for c in cats)} "
                f"fail={cycle.get('check_fail', 0)} | "
                f"TOTAL  pass={sum(life.get(f'check_pass_{c}', 0) for c in cats)} "
                f"fail={life.get('check_fail', 0)}",
                flush=True,
            )

    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [asyncio.create_task(_worker(session)) for _ in range(CHECK_CONCURRENCY)]
        stats   = asyncio.create_task(_stats_printer())
        await asyncio.gather(*workers, stats)

def _checker_pipeline_thread():
    global _check_loop
    _check_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_check_loop)
    _check_loop.run_until_complete(_checker_pipeline())

def _enqueue(addr: str):
    if _seen_dup(addr):
        return
    if _check_queue and _check_loop and not _check_loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(_check_queue.put(addr), _check_loop)
        except RuntimeError:
            pass

# ═══════════════════════════════════════════════════════════════════════════════
# CHECKER MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════
def _checker_main():
    repos, http_srcs, geonode, html = _get_my_sources()
    print(f"[{ROLE}] sources: {len(repos)} github repos, {len(http_srcs)} http, "
          f"{len(geonode)} geonode, {len(html)} html", flush=True)

    print(f"[{ROLE}] boot fetch — targeting {BOOT_FETCH_COUNT} proxies", flush=True)
    boot_batch = _bulk_fetch(repos, http_srcs, geonode, html, BOOT_FETCH_COUNT)
    print(f"[{ROLE}] boot fetch got {len(boot_batch)} — queuing for check", flush=True)
    for addr in boot_batch:
        _enqueue(addr)

    def _init_sha_cache():
        for owner, repo, path in repos:
            key = f"{owner}/{repo}/{path}"
            _, sha = _fetch_github_latest(owner, repo, path)
            if sha:
                with _sha_lock:
                    _repo_sha[key] = sha
    threading.Thread(target=_init_sha_cache, daemon=True, name="sha-init").start()

    last_commit_poll = time.time()
    cycle_offset = BOOT_FETCH_COUNT

    while True:
        time.sleep(5)
        now = time.time()
        _reset_cycle()   # cycle counters = this poll window only

        if now - last_commit_poll >= COMMIT_POLL_S:
            last_commit_poll = now
            commit_found = False
            for owner, repo, path in repos:
                new_proxies, sha = _fetch_github_commit(owner, repo, path)
                if new_proxies:
                    commit_found = True
                    print(f"[{ROLE}] new commit {owner}/{repo} → {len(new_proxies)} proxies", flush=True)
                    _log("new_commit", f"{owner}/{repo} sha={sha[:8]} count={len(new_proxies)}")
                    _inc("commits_detected")
                    for addr in new_proxies:
                        _enqueue(addr)
            if commit_found:
                continue

        q_depth = _check_queue.qsize() if _check_queue else 0
        if q_depth > 2000:
            continue

        print(f"[{ROLE}] cycle fetch — offset={cycle_offset} target={CYCLE_FETCH_COUNT}", flush=True)
        batch = _bulk_fetch(repos, http_srcs, geonode, html, CYCLE_FETCH_COUNT)
        queued = 0
        for addr in batch:
            _enqueue(addr)
            queued += 1
        cycle_offset += queued
        _inc("cycle_fetches")
        print(f"[{ROLE}] cycle queued {queued} for check", flush=True)

def _start_checker():
    t = threading.Thread(target=_checker_pipeline_thread, daemon=True, name="pipeline")
    t.start()
    deadline = time.time() + 10
    while _check_queue is None and time.time() < deadline:
        time.sleep(0.05)
    threading.Thread(target=_outbuf_sender, daemon=True, name="outbuf").start()
    threading.Thread(target=_checker_main, daemon=True, name="checker").start()
    print(f"[{ROLE}] checker started", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# POOL ROLE
# ═══════════════════════════════════════════════════════════════════════════════
def _promote(addr: str, latency_s: float):
    cat = _assign_cat(latency_s)
    if cat is None:
        _inc("pool_rejected_slow")
        return
    now = time.time()
    with _proxies_lock:
        rec = _proxies.get(addr)
        if rec is None:
            rec = {"received_at": now}
            _proxies[addr] = rec
        rec["last_checked"] = now
        rec["latency_ms"]   = int(latency_s * 1000)
        rec["label"]        = cat
        rec["next_check"]   = now + POOL_RECHECK_MIN_AGE_S
    _inc("pool_promoted")
    _log("promoted", f"{addr} {cat} {latency_s:.1f}s")
    _persist_event.set()

def _evict(addr: str, reason: str = "dead"):
    with _proxies_lock:
        existed = _proxies.pop(addr, None) is not None
    if existed:
        _inc("pool_evicted")
        _log("evicted", f"{addr} — {reason}")
        _persist_event.set()
    with _used_lock:
        _last_used.pop(addr, None)



def _pick(purpose: str = "backend", caller: str = "unknown") -> dict:
    """Pick one proxy immediately for normal/sticky requests."""
    now = time.time()

    with _proxies_lock:
        items = list(_proxies.items())

    if not items:
        return {}

    live_sorted = sorted(
        items,
        key=lambda x: x[1].get("last_checked", 0),
        reverse=True,
    )

    selected = None
    with _used_lock:
        for addr, rec in live_sorted:
            if now - _last_used.get(addr, 0) >= POOL_COOLDOWN_S:
                selected = (addr, rec)
                break

    if selected is None:
        selected = live_sorted[0]

    addr, rec = selected

    with _used_lock:
        _last_used[addr] = now

    with _proxies_lock:
        current = _proxies.get(addr)
        if current:
            current["next_check"] = now + POOL_RECHECK_MIN_AGE_S

    _inc("pool_picks")
    _inc(f"pool_picks_by_{caller}")
    _inc("pool_picks_normal")

    _log(
        "pick_normal",
        f"{addr} latency={rec.get('latency_ms', 0)/1000:.2f}s "
        f"caller={caller} purpose={purpose}",
    )

    return {
        "http": f"http://{addr}",
        "https": f"http://{addr}",
        "addr": addr,
    }


def _pick_race_candidates(
    caller: str = "unknown",
    count: int = RACE_PROXY_COUNT,
) -> list[dict]:
    """Wait 2s, then return fresh-window candidates or latest 10 live proxies."""
    window_start = time.time()
    time.sleep(RACE_WAIT_S)
    now = time.time()

    with _proxies_lock:
        items = list(_proxies.items())

    if not items:
        return []

    fresh = []
    for addr, rec in items:
        checked_at = rec.get("last_checked", 0)
        latency_s = rec.get("latency_ms", 0) / 1000.0

        if window_start <= checked_at <= now and 0 < latency_s < RACE_FRESH_MAX_S:
            fresh.append((checked_at, addr, rec))

    fresh.sort(key=lambda x: x[0], reverse=True)

    if fresh:
        selected = [(addr, rec) for _, addr, rec in fresh[:count]]
        source = "fresh_window"
    else:
        selected = sorted(
            items,
            key=lambda x: x[1].get("last_checked", 0),
            reverse=True,
        )[:count]
        source = "latest_10"

    if not selected:
        return []

    with _used_lock:
        for addr, _ in selected:
            _last_used[addr] = now

    with _proxies_lock:
        for addr, _ in selected:
            current = _proxies.get(addr)
            if current:
                current["next_check"] = now + POOL_RECHECK_MIN_AGE_S

    _inc("pool_race_sets")
    _inc(f"pool_race_sets_{source}")
    _inc("pool_race_candidates", len(selected))

    _log(
        "race_candidates",
        f"source={source} count={len(selected)} caller={caller} "
        f"addrs={','.join(a for a, _ in selected)}",
        len(selected),
    )

    return [
        {
            "http": f"http://{addr}",
            "https": f"http://{addr}",
            "addr": addr,
        }
        for addr, _ in selected
    ]


def mark_dead(addr: str):
    addr = addr.replace("http://", "").replace("https://", "").split("/")[0]
    _evict(addr, "dead (reported)")

def _snapshot_worker():
    global _snap_fresh30, _snap_fresh120, _snap_fast
    while True:
        now = time.time()
        f30, f120, fast, stale = [], [], [], []
        with _proxies_lock:
            items = list(_proxies.items())
        for a, r in items:
            lc  = r.get("last_checked", 0)
            age = (now - lc) if lc else 1e9
            if lc and now - r.get("received_at", now) > POOL_TTL_S:
                stale.append(a)
                continue
            if age <= FRESH_COLD_S:
                f120.append(a)
                if r.get("label") in ("flash", "panther"):
                    fast.append(a)
                if age <= FRESH_HOT_S:
                    f30.append(a)
        for a in stale:
            _evict(a, "ttl_expired")
        with _snap_lock:
            _snap_fresh30  = f30
            _snap_fresh120 = f120
            _snap_fast     = fast
        time.sleep(SNAP_INTERVAL_S)

async def _pool_recheck_worker():
    connector = aiohttp.TCPConnector(limit=POOL_RECHECK_BATCH + 10, ttl_dns_cache=300)
    sem = asyncio.Semaphore(POOL_RECHECK_BATCH)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await asyncio.sleep(POOL_RECHECK_INTERVAL_S)
            now = time.time()
            with _proxies_lock:
                due = [a for a, r in _proxies.items() if now >= r.get("next_check", 0)]
            random.shuffle(due)
            batch = due[:POOL_RECHECK_BATCH]
            if not batch:
                continue
            print(f"[pool] recheck {len(batch)} proxies", flush=True)

            async def _check_one(addr):
                async with sem:
                    passed, latency = await _meesho_check(session, addr)
                if passed:
                    _promote(addr, latency)
                    _inc("recheck_pass")
                else:
                    _evict(addr, "recheck_fail")
                    _inc("recheck_fail")

            await asyncio.gather(*[_check_one(a) for a in batch])

def _pool_recheck_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_pool_recheck_worker())

def _persist_worker():
    while True:
        _persist_event.wait(timeout=PERSIST_INTERVAL_S)
        _persist_event.clear()
        _save_free()
        _save_paid()

def _start_pool():
    _load_free()
    threading.Thread(target=_snapshot_worker,     daemon=True, name="snap").start()
    threading.Thread(target=_pool_recheck_thread, daemon=True, name="recheck").start()
    threading.Thread(target=_persist_worker,      daemon=True, name="persist").start()
    print(f"[pool] started — {len(_proxies)} proxies loaded", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# VPS ROLE
# ═══════════════════════════════════════════════════════════════════════════════
def _get_proxy_from_pool(cat: str = "any", purpose: str = "backend") -> dict:
    pool_url = PEERS.get("pool", "")
    if not pool_url:
        return {}
    try:
        r = requests.get(
            f"{pool_url}/pick",
            params={"cat": cat, "purpose": purpose},
            headers={"X-Secret": SHARED_SECRET},
            timeout=5,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[vps] pool /pick failed: {e}", flush=True)
    return {}


def _get_race_proxies_from_pool(count: int = RACE_PROXY_COUNT) -> list[dict]:
    pool_url = PEERS.get("pool", "")
    if not pool_url:
        return []

    try:
        r = requests.get(
            f"{pool_url}/pick-many",
            params={"count": count},
            headers={"X-Secret": SHARED_SECRET},
            timeout=RACE_WAIT_S + 3,
        )
        if r.status_code == 200:
            data = r.json()
            proxies = data.get("proxies", [])
            if isinstance(proxies, list):
                return proxies
    except Exception as e:
        print(f"[vps] pool /pick-many failed: {e}", flush=True)

    return []

def _report_dead_to_pool(addr: str):
    pool_url = PEERS.get("pool", "")
    if not pool_url:
        return
    try:
        requests.post(
            f"{pool_url}/dead",
            json={"addr": addr},
            headers={"X-Secret": SHARED_SECRET},
            timeout=3,
        )
    except Exception:
        pass

def _pick_paid() -> dict:
    with _paid_lock:
        live = [p for p in _paid_proxies.values() if p.get("alive")]
    if not live:
        return {}
    p = random.choice(live)
    return {"http": p["http"], "https": p["https"], "addr": p.get("addr", "")}

def pick_proxy_vps(tier: str = "any", cat: str = "any", purpose: str = "backend") -> dict:
    if tier == "paid":
        return _pick_paid()
    return _get_proxy_from_pool(cat, purpose)

def _check_paid_proxy(addr: str, prx: dict) -> bool:
    hdr = _meesho_headers()
    try:
        r    = requests.get(
            f"{MEESHO_API}/api/1.0/anonymous/config",
            headers=hdr, proxies=prx, timeout=10, verify=False,
        )
        enc  = r.headers.get("content-encoding", "")
        data = _decode_meesho(r.content, enc)
        xoox = data.get("xoox", {})
        if isinstance(xoox, str):
            try: xoox = json.loads(xoox)
            except: xoox = {}
        return bool(isinstance(xoox, dict) and xoox.get("xo"))
    except Exception:
        return False

def _fetch_webshare(entry: dict) -> list[dict]:
    label, key = entry["label"], entry["key"]
    out, page  = [], 1
    while True:
        try:
            r = requests.get(
                f"https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page={page}&page_size=100",
                headers={"Authorization": f"Token {key}"}, timeout=10,
            )
            if r.status_code == 401:
                print(f"[paid] {label} invalid key", flush=True)
                break
            data    = r.json()
            results = data.get("results", [])
            if not results:
                break
            for p in results:
                ip   = p.get("proxy_address", "")
                port = p.get("port", 0)
                user = p.get("username", "")
                pw   = p.get("password", "")
                addr = f"{ip}:{port}"
                url  = f"http://{user}:{pw}@{ip}:{port}" if user and pw else f"http://{ip}:{port}"
                out.append({
                    "addr": addr, "http": url, "https": url,
                    "key_label": label, "alive": None, "last_check": 0,
                })
            if not data.get("next"):
                break
            page += 1
        except Exception as e:
            print(f"[paid] fetch failed {label} p{page}: {e}", flush=True)
            break
    print(f"[paid] {label} pulled {len(out)}", flush=True)
    return out

def _paid_worker():
    _load_paid()
    with _keys_lock:
        keys = list(_keys)
    for entry in keys:
        proxies = _fetch_webshare(entry)
        with _paid_lock:
            for p in proxies:
                if p["addr"] not in _paid_proxies:
                    _paid_proxies[p["addr"]] = p

        def _check(label=entry["label"]):
            with _paid_lock:
                to_check = [(a, dict(p)) for a, p in _paid_proxies.items() if p.get("key_label") == label]
            alive = 0
            for addr, p in to_check:
                ok = _check_paid_proxy(addr, {"http": p["http"], "https": p["https"]})
                if ok: alive += 1
                with _paid_lock:
                    if addr in _paid_proxies:
                        _paid_proxies[addr]["alive"]      = ok
                        _paid_proxies[addr]["last_check"] = time.time()
            _save_paid()
            print(f"[paid] {label} ready — {alive}/{len(to_check)} alive", flush=True)

        threading.Thread(target=_check, daemon=True, name=f"paid-{entry['label']}").start()

def _pinger():
    ping_order = [
        "checker_1", "checker_2", "checker_3", "checker_4", "checker_5",
        "checker_6", "checker_7", "checker_8", "checker_9", "checker_10",
        "pool", "dashboard",
    ]
    while True:
        for role in ping_order:
            url = PEERS.get(role, "")
            if not url:
                continue
            try:
                requests.get(f"{url}/health", timeout=5)
                print(f"[pinger] {url} ok", flush=True)
            except Exception as e:
                print(f"[pinger] {url} failed: {e}", flush=True)
            time.sleep(3)
        time.sleep(PING_INTERVAL)

def _start_vps():
    _load_paid()
    _load_keys()
    threading.Thread(target=_paid_worker,    daemon=True, name="paid").start()
    threading.Thread(target=_pinger,         daemon=True, name="pinger").start()
    threading.Thread(target=_persist_worker, daemon=True, name="persist").start()
    print(f"[vps] started", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PERSIST
# ═══════════════════════════════════════════════════════════════════════════════
def _save_free():
    try:
        with _proxies_lock:
            data = {
                a: {
                    "label":        r.get("label"),
                    "latency_ms":   r.get("latency_ms"),
                    "last_checked": r.get("last_checked", 0),
                    "received_at":  r.get("received_at", 0),
                }
                for a, r in _proxies.items()
            }
        PERSIST_FREE.write_text(json.dumps(data))
    except Exception as e:
        print(f"[persist] free save failed: {e}", flush=True)

def _load_free():
    if not PERSIST_FREE.exists():
        return
    try:
        data = json.loads(PERSIST_FREE.read_text())
        now  = time.time()
        rows = data.items() if isinstance(data, dict) else [(a, {}) for a in data]
        with _proxies_lock:
            for addr, r in rows:
                if now - r.get("received_at", 0) > POOL_TTL_S:
                    continue
                _proxies[addr] = {
                    "received_at":  r.get("received_at", now),
                    "last_checked": r.get("last_checked", 0),
                    "latency_ms":   r.get("latency_ms", 0),
                    "label":        r.get("label"),
                    "next_check":   now,
                }
        print(f"[persist] loaded {len(_proxies)} free proxies", flush=True)
    except Exception as e:
        print(f"[persist] free load failed: {e}", flush=True)

def _save_paid():
    try:
        with _paid_lock:
            data = {a: dict(p) for a, p in _paid_proxies.items() if p.get("alive")}
        PERSIST_PAID.write_text(json.dumps(data))
    except Exception as e:
        print(f"[persist] paid save failed: {e}", flush=True)

def _load_paid():
    if not PERSIST_PAID.exists():
        return
    try:
        data = json.loads(PERSIST_PAID.read_text())
        with _paid_lock:
            for addr, p in data.items():
                if addr not in _paid_proxies:
                    _paid_proxies[addr] = p
        print(f"[persist] loaded {len(data)} paid proxies", flush=True)
    except Exception as e:
        print(f"[persist] paid load failed: {e}", flush=True)

def _save_keys():
    try:
        with _keys_lock:
            keys = list(_keys)
        KEYS_FILE.write_text(json.dumps(keys, indent=2))
    except Exception as e:
        print(f"[persist] keys save failed: {e}", flush=True)

def _load_keys():
    if not KEYS_FILE.exists():
        return
    try:
        data = json.loads(KEYS_FILE.read_text())
        with _keys_lock:
            _keys.extend(data)
        print(f"[persist] loaded {len(data)} keys", flush=True)
    except Exception as e:
        print(f"[persist] keys load failed: {e}", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════════
def _get_stats() -> dict:
    now = time.time()
    with _proxies_lock:
        items = list(_proxies.items())
    live = len(items)
    f30  = sum(1 for _, r in items if r.get("last_checked") and now - r["last_checked"] <= FRESH_HOT_S)
    f120 = sum(1 for _, r in items if r.get("last_checked") and now - r["last_checked"] <= FRESH_COLD_S)
    labs = defaultdict(int)
    for _, r in items:
        if r.get("label"):
            labs[r["label"]] += 1
    with _paid_lock:
        p_total = len(_paid_proxies)
        p_alive = sum(1 for p in _paid_proxies.values() if p.get("alive"))
    with _outbuf_lock: obuf = len(_outbuf)
    with _counter_lock:
        cnts_life  = dict(_counters_lifetime)
        cnts_cycle = dict(_counters_cycle)
        c_start    = _cycle_start
    with _activity_lock: acts = list(reversed(_activity_log))
    q_depth = _check_queue.qsize() if _check_queue else 0

    caller_picks = {
        "lifetime": {
            k[len("pool_picks_by_"):]: v
            for k, v in cnts_life.items()
            if k.startswith("pool_picks_by_")
        },
        "cycle": {
            k[len("pool_picks_by_"):]: v
            for k, v in cnts_cycle.items()
            if k.startswith("pool_picks_by_")
        },
    }

    return {
        "role": ROLE,
        "counters": {
            "lifetime": cnts_life,
            "cycle": cnts_cycle,
            "cycle_started_ago_s": round(time.time() - c_start, 1),
        },
        "caller_picks": caller_picks,
        "activity":     acts[:50],
        "queues":       {"check": q_depth, "outbuf": obuf},
        "free": {
            "live":       live,
            "fresh_30s":  f30,
            "fresh_120s": f120,
            "flash":      labs["flash"],
            "panther":    labs["panther"],
            "lantern":    labs["lantern"],
            "deadass":    labs["deadass"],
        },
        "paid":  {"total": p_total, "alive": p_alive},
        "peers": {role: url for role, url in PEERS.items()},
    }

# ═══════════════════════════════════════════════════════════════════════════════
# FASTAPI
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title=f"ProxyStack v2 [{ROLE}]")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_static = BASE / "static"
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")

@app.on_event("startup")
async def _startup():
    if ROLE.startswith("checker_"):
        _start_checker()
        deadline = time.time() + 10
        while _check_queue is None and time.time() < deadline:
            await asyncio.sleep(0.05)
    elif ROLE == "pool":
        _start_pool()
    elif ROLE == "dashboard":
        print(f"[dashboard] Yoru started", flush=True)
    elif ROLE == "vps":
        _start_vps()
    print(f"[ProxyStack v2] {ROLE} booted on port {PORT}", flush=True)

@app.get("/health")
def health():
    return {"ok": True, "role": ROLE}

@app.get("/stats")
async def stats():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_api_executor, _get_stats)

@app.get("/activity")
async def activity(limit: int = 50):
    with _activity_lock:
        acts = list(reversed(_activity_log))
    return acts[:limit]

@app.get("/qsize")
def qsize(x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    q = _check_queue.qsize() if _check_queue else 0
    with _outbuf_lock: ob = len(_outbuf)
    return {"check_qsize": q, "outbuf": ob, "role": ROLE}

async def _sse_generator():
    loop = asyncio.get_event_loop()
    while True:
        try:
            if ROLE == "dashboard":
                merged = {"role": ROLE, "nodes": {}, "ts": time.time()}
                for peer_role, peer_url in PEERS.items():
                    try:
                        r = await asyncio.wait_for(
                            loop.run_in_executor(
                                _api_executor,
                                lambda u=peer_url: requests.get(f"{u}/stats", timeout=2).json()
                            ),
                            timeout=3.0,
                        )
                        merged["nodes"][peer_role] = r
                    except Exception:
                        merged["nodes"][peer_role] = {"error": "unreachable"}
                yield f"data: {json.dumps(merged)}\n\n"
            else:
                data       = await asyncio.wait_for(
                    loop.run_in_executor(_api_executor, _get_stats),
                    timeout=2.0,
                )
                data["ts"] = time.time()
                yield f"data: {json.dumps(data)}\n\n"
        except asyncio.TimeoutError:
            yield ": keepalive\n\n"
        await asyncio.sleep(2)

@app.get("/stream/stats")
async def stream_stats():
    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )

# ── Pool ingest ───────────────────────────────────────────────────────────────
class IngestItem(BaseModel):
    addr:      str
    latency_s: float = 5.0

class IngestRequest(BaseModel):
    proxies: list[IngestItem]

@app.post("/ingest")
async def ingest(req: IngestRequest, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, f"ingest only on pool role, this is {ROLE}")
    for item in req.proxies:
        _promote(item.addr, item.latency_s)
    return {"ingested": len(req.proxies)}

# ── Pool pick ─────────────────────────────────────────────────────────────────
@app.get("/pick")
async def pick_route(
    request: Request,
    cat: str = "any",
    purpose: str = "backend",
    x_secret: Optional[str] = Header(None),
):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, "pick only on pool role")
    caller = _identify_caller(request)
    loop = asyncio.get_event_loop()
    p = await loop.run_in_executor(_api_executor, _pick, purpose, caller)
    if not p:
        raise HTTPException(503, "no live proxies available")
    return p

# ── Pool dead ─────────────────────────────────────────────────────────────────
class DeadReport(BaseModel):
    addr: str

@app.get("/pick-many")
async def pick_many_route(
    request: Request,
    count: int = RACE_PROXY_COUNT,
    x_secret: Optional[str] = Header(None),
):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, "pick-many only on pool role")

    count = max(1, min(count, RACE_PROXY_COUNT))
    caller = _identify_caller(request)
    loop = asyncio.get_event_loop()

    proxies = await loop.run_in_executor(
        _api_executor,
        _pick_race_candidates,
        caller,
        count,
    )

    if not proxies:
        raise HTTPException(503, "no live proxies available")

    return {"proxies": proxies}

@app.post("/dead")
async def report_dead(req: DeadReport, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, "dead report only on pool role")
    mark_dead(req.addr)
    return {"ok": True}

@app.post("/reset-cycle")
async def reset_cycle_route(x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    _reset_cycle()
    return {"ok": True, "reset_at": time.time()}

# ── VPS /request ──────────────────────────────────────────────────────────────
class ProxyRequest(BaseModel):
    url:      str
    method:   str            = "GET"
    headers:  Optional[dict] = None
    body:     Optional[dict] = None
    params:   Optional[dict] = None
    retries:  int            = 5
    timeout:  int            = 10
    tier:     str            = "any"
    category: str            = "any"

@app.post("/request")
async def proxy_request(req: ProxyRequest):
    if ROLE != "vps":
        raise HTTPException(400, "/request only on vps role")

    method = req.method.upper()
    purpose = "fod-hunt" if _is_fod_hunt(req.url) else "backend"
    skey = _sticky_key(req.headers) if purpose == "fod-hunt" else ""
    loop = asyncio.get_event_loop()

    # Sticky URLs stay single-proxy. Only explicitly raceable URLs race.
    if _is_raceable_url(req.url):
        _inc("race_requests")
        _log("race_start", f"url={req.url} method={method}")

        proxies = await loop.run_in_executor(
            _backend_executor,
            _get_race_proxies_from_pool,
            RACE_PROXY_COUNT,
        )

        if not proxies:
            raise HTTPException(503, "no proxies available for race")

        connector = aiohttp.TCPConnector(
            limit=len(proxies) + 5,
            ssl=False,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(
            connector=connector,
        ) as session:

            async def _one_shot(prx: dict):
                addr = prx.get("addr", "")
                proxy_url = prx.get("http", "")

                if not proxy_url:
                    return None

                try:
                    async with session.request(
                        method=method,
                        url=req.url,
                        headers=req.headers or {},
                        json=req.body if method in ("POST", "PUT", "PATCH") else None,
                        params=req.params,
                        proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=req.timeout),
                    ) as r:

                        raw = await r.read()
                        enc = r.headers.get("Content-Encoding", "")
                        ct = r.headers.get(
                            "Content-Type",
                            "application/octet-stream",
                        )

                        try:
                            if "gzip" in enc:
                                raw = gzip.decompress(raw)
                            elif "deflate" in enc:
                                raw = zlib.decompress(raw)
                        except Exception:
                            pass

                        if r.status < 400:
                            return {
                                "addr": addr,
                                "status": r.status,
                                "raw": raw,
                                "content_type": ct,
                            }

                except (
                    asyncio.TimeoutError,
                    aiohttp.ClientConnectionError,
                    aiohttp.ClientProxyConnectionError,
                    aiohttp.ClientError,
                ):
                    if addr:
                        await asyncio.get_running_loop().run_in_executor(
                            _backend_executor,
                            _report_dead_to_pool,
                            addr,
                        )
                except Exception:
                    pass

                return None

            tasks = [
                asyncio.create_task(_one_shot(prx))
                for prx in proxies
            ]

            try:
                for completed in asyncio.as_completed(
                    tasks,
                    timeout=req.timeout,
                ):
                    result = await completed

                    if result is None:
                        continue

                    for task in tasks:
                        if not task.done():
                            task.cancel()

                    await asyncio.gather(
                        *tasks,
                        return_exceptions=True,
                    )

                    _inc("race_wins")
                    _log(
                        "race_win",
                        f"url={req.url} proxy={result['addr']} "
                        f"status={result['status']} candidates={len(proxies)}",
                    )

                    return Response(
                        content=result["raw"],
                        status_code=result["status"],
                        media_type=result["content_type"],
                    )

            except asyncio.TimeoutError:
                pass
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()

                await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

        _inc("race_all_failed")
        raise HTTPException(502, "all race proxies failed")

    # Normal/sticky request: immediate single proxy.
    prx = None

    if skey:
        with _sticky_lock:
            st = _sticky.get(skey)
            if st and st[1] > time.time():
                prx = {
                    "http": f"http://{st[0]}",
                    "https": f"http://{st[0]}",
                    "addr": st[0],
                }
                _sticky[skey] = (
                    st[0],
                    time.time() + STICKY_TTL_S,
                )

    if prx is None:
        prx = await loop.run_in_executor(
            _backend_executor,
            pick_proxy_vps,
            req.tier,
            req.category,
            purpose,
        )

        if prx and skey:
            with _sticky_lock:
                _sticky[skey] = (
                    prx.get("addr", ""),
                    time.time() + STICKY_TTL_S,
                )

    if not prx:
        raise HTTPException(503, "no live proxies available")

    proxy_addr = prx.get(
        "addr",
        prx.get("http", ""),
    )
    proxy_url = prx["http"]

    try:
        connector = aiohttp.TCPConnector(
            ssl=False,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(
            connector=connector,
        ) as session:

            async with session.request(
                method=method,
                url=req.url,
                headers=req.headers or {},
                json=req.body if method in ("POST", "PUT", "PATCH") else None,
                params=req.params,
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=req.timeout),
            ) as r:

                raw = await r.read()
                enc = r.headers.get("Content-Encoding", "")
                ct = r.headers.get(
                    "Content-Type",
                    "application/octet-stream",
                )

                try:
                    if "gzip" in enc:
                        raw = gzip.decompress(raw)
                    elif "deflate" in enc:
                        raw = zlib.decompress(raw)
                except Exception:
                    pass

                return Response(
                    content=raw,
                    status_code=r.status,
                    media_type=ct,
                )

    except asyncio.TimeoutError:
        raise HTTPException(504, "upstream request timed out")

    except (
        aiohttp.ClientProxyConnectionError,
        aiohttp.ClientConnectionError,
    ) as e:

        await loop.run_in_executor(
            _backend_executor,
            _report_dead_to_pool,
            proxy_addr,
        )

        if skey:
            with _sticky_lock:
                _sticky.pop(skey, None)

        raise HTTPException(
            502,
            f"proxy connection failed: {e}",
        )

    except Exception as e:
        raise HTTPException(
            502,
            f"upstream request failed: {e}",
        )


# ── VPS key management ────────────────────────────────────────────────────────
class AddKeyRequest(BaseModel):
    provider: str
    key:      str
    label:    Optional[str] = ""

class RemoveKeyRequest(BaseModel):
    key: str

@app.post("/keys/add")
async def add_key(req: AddKeyRequest):
    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps role")
    auto_label = req.label or f"{req.provider}-{req.key[:8]}"
    with _keys_lock:
        if any(k["key"] == req.key for k in _keys):
            raise HTTPException(400, "key already exists")
        existing = {k["label"] for k in _keys}
        final    = auto_label
        suffix   = 2
        while final in existing:
            final = f"{auto_label}-{suffix}"
            suffix += 1
        entry = {"provider": req.provider, "key": req.key, "label": final}
        _keys.append(entry)
    _save_keys()

    def _bg():
        proxies = _fetch_webshare(entry)
        with _paid_lock:
            for p in proxies:
                if p["addr"] not in _paid_proxies:
                    _paid_proxies[p["addr"]] = p
        with _paid_lock:
            to_check = [(a, dict(px)) for a, px in _paid_proxies.items() if px.get("key_label") == final]
        alive = 0
        for addr, p in to_check:
            ok = _check_paid_proxy(addr, {"http": p["http"], "https": p["https"]})
            if ok: alive += 1
            with _paid_lock:
                if addr in _paid_proxies:
                    _paid_proxies[addr]["alive"]      = ok
                    _paid_proxies[addr]["last_check"] = time.time()
        _save_paid()
        print(f"[paid] {final} ready — {alive}/{len(to_check)} alive", flush=True)

    threading.Thread(target=_bg, daemon=True, name=f"add-key-{final}").start()
    return {"status": "ok", "label": final}

@app.delete("/keys/remove")
async def remove_key(req: RemoveKeyRequest):
    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps role")
    with _keys_lock:
        entry = next((k for k in _keys if k["key"] == req.key), None)
        if not entry:
            raise HTTPException(404, "key not found")
        label    = entry["label"]
        _keys[:] = [k for k in _keys if k["key"] != req.key]
    with _paid_lock:
        dead = [a for a, p in _paid_proxies.items() if p.get("key_label") == label]
        for a in dead:
            del _paid_proxies[a]
    _save_keys()
    _save_paid()
    return {"status": "ok", "evicted": len(dead)}

@app.get("/keys/list")
async def list_keys():
    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps role")
    with _keys_lock:
        return [
            {"label": k["label"], "provider": k["provider"], "hint": f"***{k['key'][-6:]}"}
            for k in _keys
        ]

# ── Dashboard / pool root ─────────────────────────────────────────────────────
@app.get("/")
def root():
    if ROLE in ("pool", "dashboard"):
        dash = BASE / "static" / "dashboard.html"
        if dash.exists():
            return HTMLResponse(dash.read_text())
        return HTMLResponse(
            "<h2>ProxyStack v2</h2>"
            "<p>dashboard.html not found — drop it in /static/</p>"
            "<p><a href='/stats'>stats JSON</a> | "
            "<a href='/stream/stats'>SSE stream</a></p>"
        )
    return {"role": ROLE, "status": "running"}
