#!/usr/bin/env python3
"""
ProxyStack — Distributed Proxy Gateway
=======================================
One file. 13 machines. Change ROLE env var, different behavior.

ROLES:
  fetcher_a  → Brimstone — hybrid: fetch + local T1 + local T2 (batch A sources)
  fetcher_b  → Viper     — hybrid: fetch + local T1 + local T2 (batch B sources)
  t1_a       → Sage      — TCP + Httpbin check (overflow from fetchers)
  t1_b       → Sova      — TCP + Httpbin check (overflow from fetchers)
  t1_c       → Astra     — TCP + Httpbin check (overflow from fetchers)
  t1_d       → Harbor    — TCP + Httpbin check (overflow from fetchers)
  t1_e       → Reyna     — TCP + Httpbin check (overflow from fetchers)
  t2_a       → Raze      — Meesho T2 check
  t2_b       → Killjoy   — Meesho T2 check
  t2_c       → Cypher    — Meesho T2 check
  pool       → Yoru      — Live proxy pool, /pick, /dead, persist
  dashboard  → Jett      — Stats + dashboard UI
  vps        → Oracle VPS — /request, pinger, paid proxies, main app API

Pipeline:
  Brimstone/Viper → (local T1+T2) → Yoru /ingest
                  → (overflow raw) → Sage/Sova/Astra/Harbor/Reyna
  Sage/Sova/Astra/Harbor/Reyna → Raze/Killjoy/Cypher → Yoru /ingest
  VPS → Yoru /pick
"""

import asyncio
import gzip
import hashlib
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
from urllib.parse import urlsplit

import aiohttp
import requests
import urllib3
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Header
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

# ─── Peer URLs ────────────────────────────────────────────────────────────────
PEERS = {
    "fetcher_a": os.getenv("PEER_FETCHER_A", "https://ps-brimstone.onrender.com"),
    "fetcher_b": os.getenv("PEER_FETCHER_B", "https://ps-viper.onrender.com"),
    "t1_a":      os.getenv("PEER_T1_A",      "https://ps-sage.onrender.com"),
    "t1_b":      os.getenv("PEER_T1_B",      "https://ps-sova.onrender.com"),
    "t1_c":      os.getenv("PEER_T1_C",      "https://ps-astra.onrender.com"),
    "t1_d":      os.getenv("PEER_T1_D",      "https://ps-harbor.onrender.com"),
    "t1_e":      os.getenv("PEER_T1_E",      "https://ps-reyna.onrender.com"),
    "t2_a":      os.getenv("PEER_T2_A",      "https://ps-raze.onrender.com"),
    "t2_b":      os.getenv("PEER_T2_B",      "https://ps-killjoy.onrender.com"),
    "t2_c":      os.getenv("PEER_T2_C",      "https://ps-cypher.onrender.com"),
    "pool":      os.getenv("PEER_POOL",      "https://ps-yoru.onrender.com"),
    "dashboard": os.getenv("PEER_DASHBOARD", "https://ps-jett.onrender.com"),
    "vps":       os.getenv("PEER_VPS",       "http://130.210.16.206:8080"),
}

# T1 roles for round-robin distribution
T1_ROLES = ["t1_a", "t1_b", "t1_c", "t1_d", "t1_e"]
T2_ROLES = ["t2_a", "t2_b", "t2_c"]

# ─── Pipeline routing ─────────────────────────────────────────────────────────
# Fetchers: handle locally, overflow 20% to dedicated T1s evenly
# T1s:      each drains to a T2 (round-robin)
# T2s:      all drain to pool
DOWNSTREAM = {
    "fetcher_a": [("t1_a", 0.04), ("t1_b", 0.04), ("t1_c", 0.04), ("t1_d", 0.04), ("t1_e", 0.04)],
    "fetcher_b": [("t1_a", 0.04), ("t1_b", 0.04), ("t1_c", 0.04), ("t1_d", 0.04), ("t1_e", 0.04)],
    "t1_a":      [("t2_a", 1.0)],
    "t1_b":      [("t2_b", 1.0)],
    "t1_c":      [("t2_c", 1.0)],
    "t1_d":      [("t2_a", 0.5), ("t2_b", 0.5)],
    "t1_e":      [("t2_b", 0.5), ("t2_c", 0.5)],
    "t2_a":      [("pool", 1.0)],
    "t2_b":      [("pool", 1.0)],
    "t2_c":      [("pool", 1.0)],
    "pool":      [],
    "dashboard": [],
    "vps":       [],
}

# ═══════════════════════════════════════════════════════════════════════════════
# PATHS
# ═══════════════════════════════════════════════════════════════════════════════
BASE         = Path(__file__).parent
PERSIST_FREE = BASE / "proxy_live.json"
PERSIST_PAID = BASE / "paid_live.json"
KEYS_FILE    = BASE / "keys.json"
SESSION_FILE = BASE / "session_history.json"

# ═══════════════════════════════════════════════════════════════════════════════
# TUNING
# ═══════════════════════════════════════════════════════════════════════════════
# Fetcher
FEED_TARGET        = 5_000
SRC_RAW_CAP        = 15_000
FETCH_WORKERS      = 20
GITHUB_POLL        = 120
HTTP_POLL          = 60
SCRAPER_POLL       = 120

# Fetcher-local T1 (hybrid mode)
LOCAL_T1_CONC      = 200   # concurrent httpbin checks inside fetcher
LOCAL_T1_TIMEOUT   = 20
LOCAL_T1_BATCH     = 400

# Fetcher-local T2 (hybrid mode)
LOCAL_T2_CONC      = 80
LOCAL_T2_TIMEOUT   = 12

# T1 checker (dedicated nodes)
TCP_PREFILTER_CONC = 300
T1_BATCH_SIZE      = 500
T1_TIMEOUT         = 20
INGEST_BATCH_SIZE  = 150
INGEST_INTERVAL    = 2.0

# T2 checker (dedicated nodes)
T2_CONCURRENT      = 100
T2_WORKERS         = 60
T2_TIMEOUT         = 12

# Pool
PERSIST_INTERVAL   = 60
COOLDOWN_SEC       = 8

# ─── Labels (response-time buckets) ──────────────────────────────────────────
CAT_FLASH   = 3      # < 3s   → flash
CAT_PANTHER = 5      # 3–5s   → panther
CAT_LANTERN = 7      # 5–7s   → lantern
CAT_DEAD    = 10     # 7–10s  → deadass ; >=10s rejected

# ─── Pool refresh cadence ─────────────────────────────────────────────────────
FRESH_HOT_S     = 30
FRESH_COLD_S    = 120
RECHECK_HOT_S   = 20
RECHECK_WARM_S  = 100
STALE_EVICT_S   = 150
SNAP_INTERVAL_S = 2
REFRESH_TICK_S  = 2
REFRESH_BATCH   = 400

# ─── FOD-hunt detection + sticky leases (VPS role) ───────────────────────────
FOD_HUNT_PATHS = (
    "/api/1.0/anonymous/config",
    "/api/1.0/anonymous/referral-app-install",
    "/api/1.0/anonymous/fod-personalisation",
)
STICKY_TTL_S = 90

# ─── Streaming pipeline: hash lanes, fetch cap, dedup ────────────────────────
T1_LANES = ["fetcher_a", "fetcher_b", "t1_a", "t1_b", "t1_c", "t1_d", "t1_e"]  # 7 owners
T2_LANES = ["fetcher_a", "fetcher_b", "t2_a", "t2_b", "t2_c"]                 # 5 owners
FETCH_MAX_FLEET    = 5_000
FETCH_WINDOW_S     = 300
FETCH_MAX_PER_NODE = FETCH_MAX_FLEET // 2
SEEN_TTL_S         = 180

# Pinger (VPS role)
PING_INTERVAL      = 600

# ═══════════════════════════════════════════════════════════════════════════════
# MEESHO
# ═══════════════════════════════════════════════════════════════════════════════
MEESHO_API  = "https://prod.meeshoapi.com"
MEESHO_AUTH = "32c4d8137cn9eb493a1921f203173080"
APP_ID      = "com.meesho.supply"

# ═══════════════════════════════════════════════════════════════════════════════
# T1 TARGETS (rotation)
# ═══════════════════════════════════════════════════════════════════════════════
T1_TARGETS = [
    ("http://httpbin.org/ip",        "origin"),
    ("http://api.ipify.org",         "."),
    ("http://icanhazip.com",         "."),
    ("http://ip-api.com/json",       "query"),
    ("http://checkip.amazonaws.com", "."),
    ("http://ifconfig.me/ip",        "."),
    ("http://myexternalip.com/raw",  "."),
    ("http://ipecho.net/plain",      "."),
    ("http://ident.me",              "."),
    ("http://api4.my-ip.io/ip",      "."),
    ("http://ip.seeip.org",          "."),
]
_t1_idx      = 0
_t1_idx_lock = threading.Lock()

def _next_t1():
    global _t1_idx
    with _t1_idx_lock:
        t = T1_TARGETS[_t1_idx % len(T1_TARGETS)]
        _t1_idx += 1
    return t

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCES (split between fetcher_a and fetcher_b)
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
]

_mid = len(ALL_GITHUB_REPOS) // 2
GITHUB_REPOS_A = ALL_GITHUB_REPOS[:_mid]
GITHUB_REPOS_B = ALL_GITHUB_REPOS[_mid:]

ALL_HTTP_SOURCES = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&timeout=10000&proxy_format=ipport&format=text",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&timeout=10000&country=IN&proxy_format=ipport&format=text",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&timeout=5000&proxy_format=ipport&format=text",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&timeout=10000&country=US&proxy_format=ipport&format=text",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&timeout=10000&country=DE&proxy_format=ipport&format=text",
    "https://api.openproxylist.xyz/http.txt",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://www.proxy-list.download/api/v1/get?type=https",
    "https://www.proxy-list.download/api/v1/get?type=http&anon=elite",
    "https://www.proxy-list.download/api/v1/get?type=http&anon=anonymous",
    "https://www.proxy-list.download/api/v1/get?type=http&country=IN",
    "https://spys.me/proxy.txt",
    "https://proxyspace.pro/http.txt",
    "https://proxyspace.pro/https.txt",
    "https://proxyscan.io/download?type=http",
]

HTML_SOURCES = [
    "https://free-proxy-list.net",
    "https://sslproxies.org",
    "https://us-proxy.org",
    "https://free-proxy-list.net/uk-proxy.html",
    "https://free-proxy-list.net/anonymous-proxy.html",
]

GEONODE_PAGES = [
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=http",
]

_http_mid      = len(ALL_HTTP_SOURCES) // 2
HTTP_SOURCES_A = ALL_HTTP_SOURCES[:_http_mid]
HTTP_SOURCES_B = ALL_HTTP_SOURCES[_http_mid:]

# ═══════════════════════════════════════════════════════════════════════════════
# SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════
_IP_PORT = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b")

def _parse(text: str) -> list[str]:
    return [m.group(0) for m in _IP_PORT.finditer(text)]

# Raw pool (fetchers)
_raw_lock = threading.Lock()
_raw: set[str] = set()

_src_raw_lock = threading.Lock()
_src_raw: dict[str, list] = defaultdict(list)

# ─── Live pool (pool role) — labels, not category sets ───────────────────────
_proxies_lock = threading.Lock()
_proxies: dict[str, dict] = {}   # addr -> {received_at,last_checked,latency_ms,label,next_check_at}
_live: set[str] = set()          # mirror of _proxies keys (legacy stats/persist reads)

_used_lock = threading.Lock()
_last_used: dict[str, float] = {}

_snap_lock     = threading.Lock()
_snap_fresh30:  list[str] = []
_snap_fresh120: list[str] = []
_snap_fast:     list[str] = []   # fresh120 & label in (flash, panther)

_t1_times: dict[str, float] = {}
_t1_lock = threading.Lock()

# Paid (VPS role)
_paid_lock = threading.Lock()
_paid_proxies: dict[str, dict] = {}

_keys_lock = threading.Lock()
_keys: list[dict] = []

# Sticky leases (VPS role, FOD chain)
_sticky_lock = threading.Lock()
_sticky: dict[str, tuple] = {}   # key -> (addr, expires_at)

# GitHub ETags
_sha_lock = threading.Lock()
_repo_sha: dict[str, str] = {}
_etag_lock = threading.Lock()
_http_etag: dict[str, str] = {}
_http_lmod: dict[str, str] = {}

# Activity log (last 200 events, for dashboard)
_activity_lock = threading.Lock()
_activity_log: list[dict] = []
ACTIVITY_MAX = 200

def _log_activity(event: str, detail: str = "", count: int = 0):
    entry = {
        "ts":     time.time(),
        "role":   ROLE,
        "event":  event,
        "detail": detail,
        "count":  count,
    }
    with _activity_lock:
        _activity_log.append(entry)
        if len(_activity_log) > ACTIVITY_MAX:
            del _activity_log[:-ACTIVITY_MAX]

# Ingest outbound buffer
_outbuf_lock = threading.Lock()
_outbuf: list[tuple[str, str, float]] = []

_persist_event = threading.Event()

# T1 async loop refs (dedicated T1 nodes)
_t1_queue: asyncio.Queue = None
_t1_loop: asyncio.AbstractEventLoop = None

# T2 async loop refs (dedicated T2 nodes)
_t2_queue: asyncio.Queue = None
_t2_loop: asyncio.AbstractEventLoop = None

# Local T1+T2 loop refs (fetcher hybrid)
_local_t1_queue: asyncio.Queue = None
_local_t2_queue: asyncio.Queue = None
_local_loop: asyncio.AbstractEventLoop = None

# Per-role counters (for dashboard activity panels)
_counter_lock = threading.Lock()
_counters: dict[str, int] = defaultdict(int)

def _inc(key: str, n: int = 1):
    with _counter_lock:
        _counters[key] += n

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _assign_cat(avg_sec: float) -> Optional[str]:
    if avg_sec <  CAT_FLASH:   return "flash"
    if avg_sec <  CAT_PANTHER: return "panther"
    if avg_sec <  CAT_LANTERN: return "lantern"
    if avg_sec <  CAT_DEAD:    return "deadass"
    return None

def _promote(addr: str, latency_s: float):
    """Record a successful check result. Called from /ingest (pool role)."""
    if latency_s >= CAT_DEAD:
        _evict(addr, "too slow")
        return
    now = time.time()
    with _proxies_lock:
        rec = _proxies.get(addr)
        if rec is None:
            rec = {"received_at": now}
            _proxies[addr] = rec
        rec["last_checked"]  = now
        rec["latency_ms"]    = int(latency_s * 1000)
        rec["label"]         = _assign_cat(latency_s)
        rec["next_check_at"] = now + RECHECK_WARM_S
    _live.add(addr)
    _inc("promoted")
    _persist_event.set()

def _evict(addr: str, reason: str = "dead"):
    with _proxies_lock:
        existed = _proxies.pop(addr, None) is not None
    _live.discard(addr)
    with _used_lock:
        _last_used.pop(addr, None)
    if existed:
        _inc("evicted")
        _log_activity("evicted", f"{addr} — {reason}")
        _persist_event.set()

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
        if "gzip"      in enc: raw = gzip.decompress(raw)
        elif "deflate" in enc: raw = zlib.decompress(raw)
    except Exception:
        pass
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _tcp_open(addr: str, timeout: float = 2.0) -> bool:
    try:
        ip, port = addr.rsplit(":", 1)
        with socket.create_connection((ip, int(port)), timeout=timeout):
            return True
    except Exception:
        return False

async def _tcp_open_async(addr: str) -> bool:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _tcp_open, addr)

def _weighted_pick(downstream: list[tuple[str, float]]) -> Optional[str]:
    if not downstream:
        return None
    roles   = [r for r, _ in downstream]
    weights = [w for _, w in downstream]
    return random.choices(roles, weights=weights, k=1)[0]
# ═══════════════════════════════════════════════════════════════════════════════
# PERSIST
# ═══════════════════════════════════════════════════════════════════════════════
def _save_free():
    try:
        with _proxies_lock:
            data = {a: {"label": r.get("label"), "latency_ms": r.get("latency_ms"),
                        "last_checked": r.get("last_checked", 0)} for a, r in _proxies.items()}
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
                _proxies[addr] = {
                    "received_at":   now,
                    "last_checked":  r.get("last_checked", 0),   # may be stale → not served until rechecked
                    "latency_ms":    r.get("latency_ms", 0),
                    "label":         r.get("label"),
                    "next_check_at": now,                        # recheck immediately
                }
                _live.add(addr)
        print(f"[persist] loaded {len(_proxies)} free proxies", flush=True)
        _log_activity("startup", f"loaded {len(_proxies)} persisted proxies")
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
        with _keys_lock: keys = list(_keys)
        KEYS_FILE.write_text(json.dumps(keys, indent=2))
    except Exception as e:
        print(f"[persist] keys save failed: {e}", flush=True)

def _load_keys():
    if not KEYS_FILE.exists():
        return
    try:
        data = json.loads(KEYS_FILE.read_text())
        with _keys_lock: _keys.extend(data)
        print(f"[persist] loaded {len(data)} keys", flush=True)
    except Exception as e:
        print(f"[persist] keys load failed: {e}", flush=True)

def _persist_worker():
    while True:
        _persist_event.wait(timeout=PERSIST_INTERVAL)
        _persist_event.clear()
        if ROLE in ("pool", "vps"):
            _save_free()
            _save_paid()

# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOUND INGEST SENDER
# ═══════════════════════════════════════════════════════════════════════════════
def _push_downstream(addr: str, next_role: str, t1_elapsed: float = 5.0):
    with _outbuf_lock:
        _outbuf.append((addr, next_role, t1_elapsed))

def _outbuf_sender():
    while True:
        time.sleep(INGEST_INTERVAL)
        with _outbuf_lock:
            if not _outbuf:
                continue
            batch = list(_outbuf[:INGEST_BATCH_SIZE])
            del _outbuf[:INGEST_BATCH_SIZE]

        by_role: dict[str, list] = defaultdict(list)
        for addr, role, elapsed in batch:
            by_role[role].append({"addr": addr, "t1_elapsed": elapsed})

        for role, items in by_role.items():
            url = PEERS.get(role)
            if not url:
                continue
            endpoint = f"{url}/ingest"
            try:
                r = requests.post(
                    endpoint,
                    json={"proxies": items},
                    headers={"X-Secret": SHARED_SECRET},
                    timeout=8,
                )
                print(f"[outbuf] sent {len(items)} → {role} ({r.status_code})", flush=True)
            except Exception as e:
                print(f"[outbuf] failed → {role}: {e}", flush=True)
                with _outbuf_lock:
                    for item in items:
                        _outbuf.insert(0, (item["addr"], role, item["t1_elapsed"]))

# ═══════════════════════════════════════════════════════════════════════════════
# FETCHER ROLE — Source loops
# ═══════════════════════════════════════════════════════════════════════════════
def _register(proxies: list[str], label: str):
    if not proxies:
        return
    with _src_raw_lock:
        total = sum(len(v) for v in _src_raw.values())
        if total >= SRC_RAW_CAP:
            return
        _src_raw[label].extend(proxies)

def _fetch_github(owner: str, repo: str, path: str) -> tuple[list[str], str]:
    key   = f"{owner}/{repo}/{path}"
    label = f"{owner}/{repo}"
    try:
        r      = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/commits",
            params={"path": path, "per_page": 1},
            headers={"Accept": "application/vnd.github.v3+json"},
            timeout=8,
        )
        commits = r.json()
        if not commits or not isinstance(commits, list):
            return [], label
        sha = commits[0].get("sha", "")
    except Exception:
        return [], label
    with _sha_lock:
        if sha == _repo_sha.get(key):
            return [], label
        _repo_sha[key] = sha
    try:
        r2      = requests.get(
            f"https://raw.githubusercontent.com/{owner}/{repo}/main/{path}",
            timeout=8,
        )
        proxies = _parse(r2.text)
        if proxies:
            print(f"[gh] {key} +{len(proxies)}", flush=True)
            _inc("raw_fetched", len(proxies))
            _log_activity("fetch_github", f"{label}", len(proxies))
        return proxies, label
    except Exception:
        return [], label

def _github_loop(repos: list):
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        while True:
            futs = {ex.submit(_fetch_github, o, r, p): (o, r, p) for o, r, p in repos}
            for fut in as_completed(futs):
                fresh, label = fut.result()
                if fresh:
                    with _raw_lock: _raw.update(fresh)
                    _register(fresh, label)
            time.sleep(GITHUB_POLL)

def _fetch_http(url: str) -> tuple[list[str], str]:
    label = url.split("//")[1].split("/")[0].replace("api.", "")
    hdrs  = {}
    with _etag_lock:
        if url in _http_etag: hdrs["If-None-Match"]     = _http_etag[url]
        if url in _http_lmod: hdrs["If-Modified-Since"] = _http_lmod[url]
    try:
        r = requests.get(url, headers=hdrs, timeout=8)
        if r.status_code == 304:
            return [], label
        with _etag_lock:
            if "ETag"          in r.headers: _http_etag[url] = r.headers["ETag"]
            if "Last-Modified" in r.headers: _http_lmod[url] = r.headers["Last-Modified"]
        proxies = _parse(r.text)
        if proxies:
            print(f"[http] {url[:55]} +{len(proxies)}", flush=True)
            _inc("raw_fetched", len(proxies))
            _log_activity("fetch_http", label, len(proxies))
        return proxies, label
    except Exception:
        return [], label

def _http_loop(sources: list):
    with ThreadPoolExecutor(max_workers=len(sources) + 1) as ex:
        while True:
            futs = {ex.submit(_fetch_http, url): url for url in sources}
            for fut in as_completed(futs):
                fresh, label = fut.result()
                if fresh:
                    with _raw_lock: _raw.update(fresh)
                    _register(fresh, label)
            time.sleep(HTTP_POLL)

def _fetch_geonode(url: str) -> tuple[list[str], str]:
    try:
        r       = requests.get(url, timeout=8)
        data    = r.json()
        proxies = [f"{p['ip']}:{p['port']}" for p in data.get("data", [])]
        if proxies:
            print(f"[geonode] +{len(proxies)}", flush=True)
            _inc("raw_fetched", len(proxies))
            _log_activity("fetch_geonode", "geonode.com", len(proxies))
        return proxies, "geonode"
    except Exception:
        return [], "geonode"

def _geonode_loop():
    while True:
        for url in GEONODE_PAGES:
            fresh, label = _fetch_geonode(url)
            if fresh:
                with _raw_lock: _raw.update(fresh)
                _register(fresh, label)
        time.sleep(HTTP_POLL)

def _fetch_html(url: str) -> tuple[list[str], str]:
    label = url.split("//")[1].split("/")[0]
    try:
        r    = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
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
        if out:
            print(f"[html] {label} +{len(out)}", flush=True)
            _inc("raw_fetched", len(out))
            _log_activity("fetch_html", label, len(out))
        return out, label
    except Exception:
        return [], label

def _html_loop():
    while True:
        for url in HTML_SOURCES:
            fresh, label = _fetch_html(url)
            if fresh:
                with _raw_lock: _raw.update(fresh)
                _register(fresh, label)
        time.sleep(SCRAPER_POLL)

def _fetch_checkerproxy() -> tuple[list[str], str]:
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r     = requests.get(f"https://checkerproxy.net/api/archive/{today}", timeout=8)
        data  = r.json()
        out   = [
            p["addr"] for p in data
            if p.get("addr") and re.match(r"\d+\.\d+\.\d+\.\d+:\d+", p["addr"])
        ]
        if out:
            print(f"[checkerproxy] +{len(out)}", flush=True)
            _inc("raw_fetched", len(out))
            _log_activity("fetch_checkerproxy", "checkerproxy.net", len(out))
        return out, "checkerproxy.net"
    except Exception:
        return [], "checkerproxy.net"

def _checkerproxy_loop():
    while True:
        fresh, label = _fetch_checkerproxy()
        if fresh:
            with _raw_lock: _raw.update(fresh)
            _register(fresh, label)
        time.sleep(3600)

# ═══════════════════════════════════════════════════════════════════════════════
# FETCHER HYBRID — Local T1 + T2 pipeline inside fetcher roles
# ═══════════════════════════════════════════════════════════════════════════════
async def _local_t1_check(session: aiohttp.ClientSession, addr: str) -> tuple[bool, float]:
    url, keyword = _next_t1()
    t0 = asyncio.get_event_loop().time()
    try:
        async with session.get(
            url,
            proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=LOCAL_T1_TIMEOUT),
            ssl=False,
        ) as r:
            if r.status == 200:
                text    = await r.text()
                elapsed = asyncio.get_event_loop().time() - t0
                if keyword in text:
                    return True, elapsed
    except Exception:
        pass
    return False, 0.0

async def _local_t2_check(session: aiohttp.ClientSession, addr: str) -> tuple[bool, float]:
    hdr = _meesho_headers()
    t0  = asyncio.get_event_loop().time()
    try:
        async with session.get(
            f"{MEESHO_API}/api/1.0/anonymous/config",
            headers=hdr,
            proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=LOCAL_T2_TIMEOUT),
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

async def _local_pipeline_main():
    """Local T1→T2 pipeline running inside fetcher (Brimstone/Viper)."""
    global _local_t1_queue, _local_t2_queue

    _local_t1_queue = asyncio.Queue(maxsize=30_000)
    _local_t2_queue = asyncio.Queue(maxsize=5_000)
    downstream      = DOWNSTREAM.get(ROLE, [])

    tcp_sem = asyncio.Semaphore(LOCAL_T1_CONC)
    t1_sem  = asyncio.Semaphore(LOCAL_T1_CONC)
    t2_sem  = asyncio.Semaphore(LOCAL_T2_CONC)

    t1_connector = aiohttp.TCPConnector(limit=LOCAL_T1_CONC + 50, ttl_dns_cache=300)
    t2_connector = aiohttp.TCPConnector(limit=LOCAL_T2_CONC + 20, ttl_dns_cache=300)

    async def _t1_worker(session):
        while True:
            addr = await _local_t1_queue.get()
            async with tcp_sem:
                open_ = await _tcp_open_async(addr)
            if not open_:
                _inc("t1_tcp_fail")
                continue
            async with t1_sem:
                passed, elapsed = await _local_t1_check(session, addr)
            if passed:
                _inc("t1_pass")
                _log_activity("t1_pass", addr)
                await _local_t2_queue.put((addr, elapsed))
            else:
                _inc("t1_fail")

    async def _t2_worker(session):
        while True:
            addr, t1_elapsed = await _local_t2_queue.get()
            async with t2_sem:
                passed, t2_elapsed = await _local_t2_check(session, addr)
            if passed:
                avg = (t1_elapsed + t2_elapsed) / 2.0
                cat = _assign_cat(avg)
                if cat:
                    _inc(f"t2_pass_{cat}")
                    _log_activity("t2_pass", f"{addr} → {cat} ({avg:.1f}s)")
                    _push_downstream(addr, "pool", avg)
            else:
                _inc("t2_fail")

    async def _feeder():
        """Pull from _src_raw and push to local T1 queue."""
        while True:
            with _src_raw_lock:
                all_proxies: list[str] = []
                for pool in _src_raw.values():
                    all_proxies.extend(pool)

            if not all_proxies:
                await asyncio.sleep(2)
                continue

            random.shuffle(all_proxies)
            batch = all_proxies[:FEED_TARGET]
            sent  = 0

            for addr in batch:
                if _local_t1_queue.qsize() < 25_000:
                    await _local_t1_queue.put(addr)
                    sent += 1
                else:
                    # queue full — overflow to dedicated T1s
                    target = _weighted_pick(downstream)
                    if target:
                        _push_downstream(addr, target, 0.0)
                    sent += 1

            sent_set = set(batch)
            with _src_raw_lock:
                for label in list(_src_raw.keys()):
                    _src_raw[label] = [p for p in _src_raw[label] if p not in sent_set]

            if sent:
                print(f"[{ROLE}] feeder → {sent} to local T1 queue", flush=True)

            q = _local_t1_queue.qsize()
            if q > 20_000:  await asyncio.sleep(3)
            elif q > 8_000: await asyncio.sleep(1)
            else:           await asyncio.sleep(0.3)

    async def _stats_printer():
        while True:
            await asyncio.sleep(15)
            q1 = _local_t1_queue.qsize()
            q2 = _local_t2_queue.qsize()
            with _counter_lock:
                cnts = dict(_counters)
            print(
                f"[{ROLE}] t1_q={q1} t2_q={q2} "
                f"raw={cnts.get('raw_fetched',0)} "
                f"t1_pass={cnts.get('t1_pass',0)} "
                f"t2_pass_flash={cnts.get('t2_pass_flash',0)} "
                f"t2_pass_panther={cnts.get('t2_pass_panther',0)} "
                f"t2_pass_lantern={cnts.get('t2_pass_lantern',0)}",
                flush=True,
            )

    async with (
        aiohttp.ClientSession(connector=t1_connector) as t1_sess,
        aiohttp.ClientSession(connector=t2_connector) as t2_sess,
    ):
        tasks = (
            [asyncio.create_task(_t1_worker(t1_sess)) for _ in range(LOCAL_T1_BATCH)]
            + [asyncio.create_task(_t2_worker(t2_sess)) for _ in range(LOCAL_T2_CONC)]
            + [asyncio.create_task(_feeder()),
               asyncio.create_task(_stats_printer())]
        )
        await asyncio.gather(*tasks)

def _local_pipeline_thread():
    global _local_loop
    _local_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_local_loop)
    _local_loop.run_until_complete(_local_pipeline_main())

def _push_to_local_t1(addr: str):
    if _local_t1_queue and _local_loop and not _local_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_local_t1_queue.put(addr), _local_loop)

def _start_fetcher():
    repos     = GITHUB_REPOS_A if ROLE == "fetcher_a" else GITHUB_REPOS_B
    http_srcs = HTTP_SOURCES_A if ROLE == "fetcher_a" else HTTP_SOURCES_B

    threading.Thread(target=_github_loop,       args=(repos,),     daemon=True, name="gh").start()
    threading.Thread(target=_http_loop,         args=(http_srcs,), daemon=True, name="http").start()
    threading.Thread(target=_geonode_loop,                          daemon=True, name="geonode").start()
    threading.Thread(target=_html_loop,                             daemon=True, name="html").start()
    threading.Thread(target=_checkerproxy_loop,                     daemon=True, name="checkerproxy").start()
    threading.Thread(target=_local_pipeline_thread,                 daemon=True, name="local-pipeline").start()
    threading.Thread(target=_outbuf_sender,                         daemon=True, name="outbuf").start()
    print(f"[{ROLE}] hybrid fetcher started", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# T1 CHECKER ROLE (dedicated — Sage/Sova/Astra/Harbor/Reyna)
# ═══════════════════════════════════════════════════════════════════════════════
async def _t1_check(session: aiohttp.ClientSession, addr: str) -> tuple[bool, float]:
    url, keyword = _next_t1()
    t0 = asyncio.get_event_loop().time()
    try:
        async with session.get(
            url,
            proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=T1_TIMEOUT),
            ssl=False,
        ) as r:
            if r.status == 200:
                text    = await r.text()
                elapsed = asyncio.get_event_loop().time() - t0
                if keyword in text:
                    return True, elapsed
    except Exception:
        pass
    return False, 0.0

async def _t1_checker_main():
    global _t1_queue
    _t1_queue  = asyncio.Queue(maxsize=20_000)
    tcp_sem    = asyncio.Semaphore(TCP_PREFILTER_CONC)
    t1_sem     = asyncio.Semaphore(T1_BATCH_SIZE)
    downstream = DOWNSTREAM.get(ROLE, [])
    connector  = aiohttp.TCPConnector(
        limit=T1_BATCH_SIZE + 100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async def _worker(session):
        while True:
            addr = await _t1_queue.get()
            async with tcp_sem:
                open_ = await _tcp_open_async(addr)
            if not open_:
                _inc("t1_tcp_fail")
                continue
            async with t1_sem:
                passed, elapsed = await _t1_check(session, addr)
            if passed:
                _inc("t1_pass")
                _log_activity("t1_pass", addr)
                target = _weighted_pick(downstream)
                if target:
                    _push_downstream(addr, target, elapsed)
            else:
                _inc("t1_fail")

    async with aiohttp.ClientSession(connector=connector) as session:
        workers    = [asyncio.create_task(_worker(session)) for _ in range(T1_BATCH_SIZE)]
        stats_task = asyncio.create_task(_t1_stats_printer())
        await asyncio.gather(*workers, stats_task)

async def _t1_stats_printer():
    while True:
        await asyncio.sleep(15)
        q = _t1_queue.qsize() if _t1_queue else 0
        with _counter_lock: cnts = dict(_counters)
        print(
            f"[{ROLE}] t1_q={q} "
            f"pass={cnts.get('t1_pass',0)} fail={cnts.get('t1_fail',0)}",
            flush=True,
        )

def _t1_checker_thread():
    global _t1_loop
    _t1_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_t1_loop)
    _t1_loop.run_until_complete(_t1_checker_main())

def _push_to_t1(addr: str):
    if _t1_queue and _t1_loop and not _t1_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_t1_queue.put(addr), _t1_loop)

def _start_t1_checker():
    threading.Thread(target=_t1_checker_thread, daemon=True, name="t1-checker").start()
    threading.Thread(target=_outbuf_sender,      daemon=True, name="outbuf").start()
    print(f"[{ROLE}] T1 checker started", flush=True)
# ═══════════════════════════════════════════════════════════════════════════════
# T2 CHECKER ROLE (dedicated — Raze/Killjoy/Cypher)
# ═══════════════════════════════════════════════════════════════════════════════
async def _t2_check(session: aiohttp.ClientSession, addr: str) -> tuple[bool, float]:
    hdr = _meesho_headers()
    t0  = asyncio.get_event_loop().time()
    try:
        async with session.get(
            f"{MEESHO_API}/api/1.0/anonymous/config",
            headers=hdr,
            proxy=f"http://{addr}",
            timeout=aiohttp.ClientTimeout(total=T2_TIMEOUT),
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

async def _t2_checker_main():
    global _t2_queue
    _t2_queue  = asyncio.Queue(maxsize=5_000)
    t2_sem     = asyncio.Semaphore(T2_CONCURRENT)
    downstream = DOWNSTREAM.get(ROLE, [])
    connector  = aiohttp.TCPConnector(
        limit=T2_CONCURRENT + 50,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async def _worker(session):
        while True:
            addr, t1_elapsed = await _t2_queue.get()
            async with t2_sem:
                passed, t2_elapsed = await _t2_check(session, addr)
            if passed:
                avg = (t1_elapsed + t2_elapsed) / 2.0
                cat = _assign_cat(avg)
                if cat is None:
                    _inc("t2_rejected")
                    continue
                _inc(f"t2_pass_{cat}")
                _log_activity("t2_pass", f"{addr} → {cat} ({avg:.1f}s)")
                target = _weighted_pick(downstream)
                if target:
                    _push_downstream(addr, target, avg)
            else:
                _inc("t2_fail")

    async with aiohttp.ClientSession(connector=connector) as session:
        workers    = [asyncio.create_task(_worker(session)) for _ in range(T2_WORKERS)]
        stats_task = asyncio.create_task(_t2_stats_printer())
        await asyncio.gather(*workers, stats_task)

async def _t2_stats_printer():
    while True:
        await asyncio.sleep(15)
        q = _t2_queue.qsize() if _t2_queue else 0
        with _counter_lock: cnts = dict(_counters)
        print(
            f"[{ROLE}] t2_q={q} "
            f"flash={cnts.get('t2_pass_flash',0)} "
            f"panther={cnts.get('t2_pass_panther',0)} "
            f"lantern={cnts.get('t2_pass_lantern',0)} "
            f"deadass={cnts.get('t2_pass_deadass',0)} "
            f"fail={cnts.get('t2_fail',0)}",
            flush=True,
        )

def _t2_checker_thread():
    global _t2_loop
    _t2_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_t2_loop)
    _t2_loop.run_until_complete(_t2_checker_main())

def _push_to_t2(addr: str, t1_elapsed: float = 5.0):
    if _t2_queue and _t2_loop and not _t2_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_t2_queue.put((addr, t1_elapsed)), _t2_loop)

def _start_t2_checker():
    threading.Thread(target=_t2_checker_thread, daemon=True, name="t2-checker").start()
    threading.Thread(target=_outbuf_sender,      daemon=True, name="outbuf").start()
    print(f"[{ROLE}] T2 checker started", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# POOL ROLE (Yoru)
# ═══════════════════════════════════════════════════════════════════════════════
def _pick(purpose: str = "backend") -> dict:
    now = time.time()
    with _snap_lock:
        pool = (_snap_fast or _snap_fresh120) if purpose == "fod-hunt" else _snap_fresh30
        if not pool:
            return {}
        addr = random.choice(pool)
    with _used_lock:
        if now - _last_used.get(addr, 0) < COOLDOWN_SEC:
            with _snap_lock:
                for _ in range(6):
                    if not pool:
                        break
                    alt = random.choice(pool)
                    if now - _last_used.get(alt, 0) >= COOLDOWN_SEC:
                        addr = alt
                        break
        _last_used[addr] = now
    with _proxies_lock:
        r = _proxies.get(addr)
        if r:
            r["next_check_at"] = now + RECHECK_HOT_S
    _inc("picks_served")
    _log_activity("pick_served", addr)
    return {"http": f"http://{addr}", "https": f"http://{addr}", "addr": addr}

def mark_dead(addr: str):
    addr = addr.replace("http://", "").replace("https://", "").split("/")[0]
    _evict(addr, "dead (connect/timeout)")

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
            if lc and age > STALE_EVICT_S:
                stale.append(a)
                continue
            if age <= FRESH_COLD_S:
                f120.append(a)
                if r.get("label") in ("flash", "panther"):
                    fast.append(a)
                if age <= FRESH_HOT_S:
                    f30.append(a)
        for a in stale:
            _evict(a, "stale")
        with _snap_lock:
            _snap_fresh30, _snap_fresh120, _snap_fast = f30, f120, fast
        time.sleep(SNAP_INTERVAL_S)

def _refresh_worker():
    while True:
        now = time.time()
        with _proxies_lock:
            due = [a for a, r in _proxies.items() if now >= r.get("next_check_at", 0)]
        random.shuffle(due)
        for a in due[:REFRESH_BATCH]:
            _push_downstream(a, random.choice(T2_ROLES), 5.0)
            with _proxies_lock:
                r = _proxies.get(a)
                if r:
                    r["next_check_at"] = now + RECHECK_WARM_S
        time.sleep(REFRESH_TICK_S)

def _start_pool():
    _load_free()
    threading.Thread(target=_snapshot_worker, daemon=True, name="snap").start()
    threading.Thread(target=_refresh_worker,  daemon=True, name="refresh").start()
    threading.Thread(target=_persist_worker,  daemon=True, name="persist").start()
    threading.Thread(target=_outbuf_sender,   daemon=True, name="outbuf").start()
    print(f"[{ROLE}] pool (Yoru) started — {len(_live)} proxies loaded", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# VPS ROLE
# ═══════════════════════════════════════════════════════════════════════════════
_api_executor     = ThreadPoolExecutor(max_workers=64,  thread_name_prefix="api")
_backend_executor = ThreadPoolExecutor(max_workers=128, thread_name_prefix="backend")

def _is_fod_hunt(url: str) -> bool:
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
    ping_targets = [url for role, url in PEERS.items() if role != "vps"]
    while True:
        time.sleep(PING_INTERVAL)
        for url in ping_targets:
            try:
                requests.get(f"{url}/health", timeout=5)
                print(f"[pinger] {url} ok", flush=True)
            except Exception as e:
                print(f"[pinger] {url} failed: {e}", flush=True)

def _start_vps():
    _load_paid()
    _load_keys()
    threading.Thread(target=_paid_worker,    daemon=True, name="paid").start()
    threading.Thread(target=_pinger,         daemon=True, name="pinger").start()
    threading.Thread(target=_persist_worker, daemon=True, name="persist").start()
    print(f"[{ROLE}] VPS started", flush=True)
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
    with _raw_lock:  rn = len(_raw)
    with _paid_lock:
        p_total = len(_paid_proxies)
        p_alive = sum(1 for p in _paid_proxies.values() if p.get("alive"))
    with _outbuf_lock: obuf = len(_outbuf)
    with _counter_lock: cnts = dict(_counters)
    with _activity_lock: acts = list(reversed(_activity_log))

    return {
        "role":     ROLE,
        "counters": cnts,
        "activity": acts[:50],
        "free": {
            "live": live, "raw": rn, "outbuf": obuf,
            "fresh_30s": f30, "fresh_120s": f120,
            "flash": labs["flash"], "panther": labs["panther"],
            "lantern": labs["lantern"], "deadass": labs["deadass"],
        },
        "paid":    {"total": p_total, "alive": p_alive},
        "peers":   {role: url for role, url in PEERS.items()},
    }

# ═══════════════════════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title=f"ProxyStack [{ROLE}]")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_static = BASE / "static"
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")

# ─── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def _startup():
    if ROLE in ("fetcher_a", "fetcher_b"):
        _start_fetcher()
        deadline = time.time() + 10
        while _local_t1_queue is None and time.time() < deadline:
            await asyncio.sleep(0.05)

    elif ROLE in ("t1_a", "t1_b", "t1_c", "t1_d", "t1_e"):
        _start_t1_checker()
        deadline = time.time() + 10
        while _t1_queue is None and time.time() < deadline:
            await asyncio.sleep(0.05)

    elif ROLE in ("t2_a", "t2_b", "t2_c"):
        _start_t2_checker()
        deadline = time.time() + 10
        while _t2_queue is None and time.time() < deadline:
            await asyncio.sleep(0.05)

    elif ROLE == "pool":
        _start_pool()

    elif ROLE == "dashboard":
        print(f"[{ROLE}] Jett dashboard started", flush=True)

    elif ROLE == "vps":
        _start_vps()

    print(f"[ProxyStack] {ROLE} booted on port {PORT}", flush=True)

# ─── Health ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "role": ROLE}

# ─── Stats ────────────────────────────────────────────────────────────────────
@app.get("/stats")
async def stats():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_api_executor, _get_stats)

# ─── Activity log ─────────────────────────────────────────────────────────────
@app.get("/activity")
async def activity(limit: int = 50):
    with _activity_lock:
        acts = list(reversed(_activity_log))
    return acts[:limit]

# ─── SSE stats stream ─────────────────────────────────────────────────────────
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

# ─── Ingest ───────────────────────────────────────────────────────────────────
class IngestItem(BaseModel):
    addr:       str
    t1_elapsed: float = 5.0

class IngestRequest(BaseModel):
    proxies: list[IngestItem]

@app.post("/ingest")
async def ingest(req: IngestRequest, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")

    if ROLE in ("t1_a", "t1_b", "t1_c", "t1_d", "t1_e"):
        for item in req.proxies:
            _push_to_t1(item.addr)
        return {"queued": len(req.proxies)}

    elif ROLE in ("t2_a", "t2_b", "t2_c"):
        for item in req.proxies:
            _push_to_t2(item.addr, item.t1_elapsed)
        return {"queued": len(req.proxies)}

    elif ROLE == "pool":
        for item in req.proxies:
            _promote(item.addr, item.t1_elapsed)
        return {"queued": len(req.proxies)}

    raise HTTPException(400, f"role {ROLE} does not accept ingest")

# ─── Pick ─────────────────────────────────────────────────────────────────────
@app.get("/pick")
async def pick_route(cat: str = "any", purpose: str = "backend",
                     x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, "pick only available on pool role")
    loop = asyncio.get_event_loop()
    p = await loop.run_in_executor(_api_executor, _pick, purpose)
    if not p:
        raise HTTPException(503, "no live proxies available")
    return p

# ─── Dead report ──────────────────────────────────────────────────────────────
class DeadReport(BaseModel):
    addr: str

@app.post("/dead")
async def report_dead(req: DeadReport, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "pool":
        raise HTTPException(400, "dead report only on pool role")
    mark_dead(req.addr)
    return {"ok": True}

# ─── Backend request (VPS) ───────────────────────────────────────────────────
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
        raise HTTPException(400, "/request only available on vps role")
    loop     = asyncio.get_event_loop()
    method   = req.method.upper()
    last_err = None

    purpose = "fod-hunt" if _is_fod_hunt(req.url) else "backend"
    skey    = _sticky_key(req.headers) if purpose == "fod-hunt" else ""

    for attempt in range(req.retries):
        prx = None
        if skey:                                  # reuse the shot's pinned proxy
            with _sticky_lock:
                s = _sticky.get(skey)
                if s and s[1] > time.time():
                    prx = {"http": f"http://{s[0]}", "https": f"http://{s[0]}", "addr": s[0]}
        if prx is None:
            prx = await loop.run_in_executor(
                _backend_executor, pick_proxy_vps, req.tier, req.category, purpose
            )
            if prx and skey:
                with _sticky_lock:
                    _sticky[skey] = (prx.get("addr", ""), time.time() + STICKY_TTL_S)
        if not prx:
            await asyncio.sleep(0.5)
            continue

        proxy_addr = prx.get("addr", prx.get("http", ""))
        try:
            def _fire():
                return requests.request(
                    method=method,
                    url=req.url,
                    headers=req.headers or {},
                    json=req.body if method in ("POST", "PUT", "PATCH") else None,
                    params=req.params,
                    proxies={"http": prx["http"], "https": prx["https"]},
                    verify=False,
                    timeout=req.timeout,
                )
            r   = await loop.run_in_executor(_backend_executor, _fire)
            enc = r.headers.get("content-encoding", "")
            raw = r.content
            try:
                if "gzip"      in enc: raw = gzip.decompress(raw)
                elif "deflate" in enc: raw = zlib.decompress(raw)
            except Exception:
                pass
            ct = r.headers.get("content-type", "application/octet-stream")
            return Response(content=raw, status_code=r.status_code, media_type=ct)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_err = str(e)
            await loop.run_in_executor(_backend_executor, _report_dead_to_pool, proxy_addr)
            if skey:
                with _sticky_lock:
                    _sticky.pop(skey, None)       # drop lease → re-pick fresh proxy
            print(f"[vps] attempt {attempt+1}/{req.retries} via {proxy_addr}: {e}", flush=True)
        except Exception as e:                     # non-proxy error — don't blame the proxy
            last_err = str(e)
            print(f"[vps] attempt {attempt+1}/{req.retries} err: {e}", flush=True)

    raise HTTPException(502, f"all {req.retries} attempts failed. last: {last_err}")

# ─── Key management (VPS) ────────────────────────────────────────────────────
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
        label   = entry["label"]
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
        return [{"label": k["label"], "provider": k["provider"], "hint": f"***{k['key'][-6:]}"} for k in _keys]

# ─── Dashboard root ───────────────────────────────────────────────────────────
@app.get("/")
def root():
    if ROLE == "dashboard":
        dash = BASE / "static" / "dashboard.html"
        if dash.exists():
            return HTMLResponse(dash.read_text())
        return HTMLResponse("<h2>ProxyStack — Jett Dashboard</h2><p>dashboard.html not found in /static/</p>")
    return {"role": ROLE, "status": "running"}