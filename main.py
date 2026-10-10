"""
ProxyStack v2 — Simplified Distributed Proxy Gateway
=====================================================
One file. Multiple machines. Change ROLE env var, different behavior.

ROLES:
  checker_1  through checker_10 → fetch from source slice, check meesho, push to pool
  pool       → Omen  — ingest, pick, dead, recheck, persist, dashboard SSE
  dashboard  → Yoru  — stats aggregator + dashboard UI
  vps        → Oracle — /request, pinger, premium providers, main app API

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

VPS (premium providers):
  read    → free race (3s, 6 parallel) → premium fallback (ScraperAPI → ScrapeOps → ScrapingAnt)
  write   → premium only (ScrapingAnt serial → ScraperAPI → ScrapeOps)
  sticky  → premium sticky pin (ScraperAPI → ScrapeOps); login + checkout
  free_only → single free proxy, no race, no fallback (FOD free shots)
  premium → premium rotating, no race, no free (FOD premium shots, anon session xo)

Credit sync:
  POST /keys/sync  → pulls real credits from each provider API, updates keys.json
                     credit_source becomes "api" for synced keys, "local" otherwise
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
# CALLER IDENTIFICATION
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
KEYS_FILE    = BASE / "keys.json"

# ═══════════════════════════════════════════════════════════════════════════════
# TUNING
# ═══════════════════════════════════════════════════════════════════════════════
BOOT_FETCH_COUNT    = 500
COMMIT_FETCH_COUNT  = 1000
CYCLE_FETCH_COUNT   = 500
COMMIT_POLL_S       = 60
CHECK_CONCURRENCY   = 150
CHECK_TIMEOUT_S     = 12
FETCH_WORKERS       = 20
SEEN_TTL_S          = 3600
INGEST_BATCH_SIZE   = 100
INGEST_INTERVAL_S   = 1.0

POOL_RECHECK_INTERVAL_S = 480
POOL_RECHECK_MIN_AGE_S  = 300
POOL_RECHECK_BATCH      = 80
POOL_TTL_S              = 900
POOL_COOLDOWN_S         = 8

PICK_WAIT_S          = 0.0
PICK_FAST_MAX_S      = 4.0
PICK_FALLBACK_COUNT  = 10

SNAP_INTERVAL_S         = 2
PERSIST_INTERVAL_S      = 60
CAT_FLASH   = 3.0
CAT_PANTHER = 5.0
CAT_LANTERN = 7.0
CAT_DEAD    = 10.0

FRESH_HOT_S  = 30
FRESH_COLD_S = 120

MEESHO_API  = "https://prod.meeshoapi.com"
MEESHO_AUTH = "32c4d8137cn9eb493a1921f203173080"
APP_ID      = "com.meesho.supply"

STICKY_TTL_S = 900
PING_INTERVAL = 240

FOD_HUNT_PATHS = (
    "/api/1.0/anonymous/config",
    "/api/1.0/anonymous/referral-app-install",
    "/api/1.0/anonymous/fod-personalisation",
)

# ═══════════════════════════════════════════════════════════════════════════════
# URL ROUTING
# ═══════════════════════════════════════════════════════════════════════════════

RACE_WAIT_S      = 3.0
RACE_FRESH_MAX_S = 4.0
RACE_PROXY_COUNT = 6

STICKY_URL_FRAGMENTS = [
    "/api/2.0/user/login",
    "/api/1.0/cart/paymentinfo",
    "/api/4.0/preorders",
]
STICKY_EXACT_PATHS = [
    "/api/3.0/order",
]

WRITE_URL_FRAGMENTS = [
    "/api/1.0/cart/add",
    "/api/1.0/cart/remove",
    "/api/1.0/cart/location",
    "/api/2.0/addresses",
    "/api/1.0/user/delivery-location",
    "/api/2.0/orders/",
]

# FOD URLs are NOT raceable — index.py fires them in parallel with explicit
# tier="free_only" or tier="premium". Anon session (search/recs) uses these
# same URLs but sends tier="premium" explicitly. If tier="any" arrives for
# a FOD URL, default to free_only (never race, never burn premium).
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
]


def _url_path(url: str) -> str:
    try:
        return urlsplit(url).path.rstrip("/") or "/"
    except Exception:
        return ""


def _is_sticky_url(url: str) -> bool:
    for frag in STICKY_URL_FRAGMENTS:
        if frag in url:
            return True
    return _url_path(url) in STICKY_EXACT_PATHS


def _is_write_url(url: str) -> bool:
    if _is_sticky_url(url):
        return False
    return any(frag in url for frag in WRITE_URL_FRAGMENTS)


def _is_raceable_url(url: str) -> bool:
    if _is_sticky_url(url) or _is_write_url(url):
        return False
    return any(frag in url for frag in RACEABLE_URL_FRAGMENTS)


def _plan_for_url(url: str) -> str:
    if _is_sticky_url(url):
        return "sticky"
    if _is_write_url(url):
        return "write"
    return "read"


def _is_fod_url(url: str) -> bool:
    path = _url_path(url)
    return any(path.startswith(p) for p in FOD_HUNT_PATHS)


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

_keys_lock = threading.Lock()
_keys: list[dict] = []

_sticky_lock = threading.Lock()
_sticky_sessions: dict[str, dict] = {}

_seen_lock = threading.Lock()
_seen: dict[str, float] = {}

_outbuf_lock = threading.Lock()
_outbuf: list[tuple[str, float]] = []

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

_api_executor     = ThreadPoolExecutor(max_workers=8,  thread_name_prefix="api")
_backend_executor = ThreadPoolExecutor(max_workers=24, thread_name_prefix="backend")

# ═══════════════════════════════════════════════════════════════════════════════
# PREMIUM PROVIDERS — ScrapingAnt / ScraperAPI / ScrapeOps
# ═══════════════════════════════════════════════════════════════════════════════
PROVIDERS = ("scrapingant", "scraperapi", "scrapeops")

DEFAULT_CREDIT_LIMITS = {
    "scrapingant": 10_000,
    "scraperapi":  1_000,
    "scrapeops":   1_000,
}

SCRAPINGANT_ENDPOINT = "https://api.scrapingant.com/v2/general"
SCRAPERAPI_ENDPOINT  = "https://api.scraperapi.com/"
SCRAPEOPS_ENDPOINT   = "https://proxy.scrapeops.io/v1/"

SCRAPINGANT_USAGE_URL = "https://api.scrapingant.com/v2/usage"
SCRAPERAPI_USAGE_URL  = "https://api.scraperapi.com/account"
SCRAPEOPS_USAGE_URL   = "https://backend.scrapeops.io/v1/proxy/account/usage"

PREMIUM_TIMEOUT_S = 12
USAGE_TIMEOUT_S   = 8

_scrapingant_sem = threading.Semaphore(1)


def _key_is_live(k: dict) -> bool:
    reset = float(k.get("reset_at") or 0)
    if reset and time.time() >= reset:
        k["credits_used"] = 0
        k["reset_at"]     = 0
    limit = int(k.get("credits_limit")
                or DEFAULT_CREDIT_LIMITS.get(k.get("provider", ""), 0))
    used  = int(k.get("credits_used") or 0)
    return used < limit


def _pick_premium_key(provider: str) -> Optional[dict]:
    with _keys_lock:
        candidates = [k for k in _keys if k.get("provider") == provider]
        live       = [k for k in candidates if _key_is_live(k)]
    if not live:
        return None
    live.sort(key=lambda k: float(k.get("last_used") or 0))
    return live[0]


def _get_key_by_label(provider: str, label: str) -> Optional[dict]:
    with _keys_lock:
        for k in _keys:
            if k.get("provider") == provider and k.get("label") == label:
                return k if _key_is_live(k) else None
    return None


def _mark_key_used(key_dict: dict, credits: int = 1):
    with _keys_lock:
        key_dict["credits_used"] = int(key_dict.get("credits_used") or 0) + credits
        key_dict["last_used"]    = time.time()


def _exhaust_key(key_dict: dict):
    limit = int(key_dict.get("credits_limit")
                or DEFAULT_CREDIT_LIMITS.get(key_dict.get("provider", ""), 0))
    with _keys_lock:
        key_dict["credits_used"] = limit
        key_dict["last_used"]    = time.time()


def _premium_response(r: requests.Response, key: dict, provider: str) -> dict:
    if r.status_code in (401, 403, 429):
        _exhaust_key(key)
        return {
            "success": False,
            "error":   f"{provider}: http {r.status_code}",
            "rotate":  True,
        }

    _mark_key_used(key, credits=1)

    if r.status_code >= 400:
        return {
            "success": False,
            "error":   f"{provider}: http {r.status_code}",
            "raw":     r.text[:300],
        }

    try:
        data = r.json()
    except Exception:
        data = r.text

    return {"success": True, "data": data}


def _call_scrapingant(method: str, url: str, headers: dict, body=None,
                     session=None, key_label=None) -> dict:
    if key_label:
        key = _get_key_by_label("scrapingant", key_label)
    else:
        key = _pick_premium_key("scrapingant")
    if not key:
        return {"success": False, "error": "scrapingant: no live keys"}

    ant_headers = {"x-api-key": key["key"]}
    for k, v in (headers or {}).items():
        ant_headers[f"ant-{k}"] = v

    params = {
        "url":                url,
        "browser":            "false",
        "return_page_source": "true",
    }

    with _scrapingant_sem:
        try:
            if method.upper() == "GET":
                r = requests.get(
                    SCRAPINGANT_ENDPOINT, params=params,
                    headers=ant_headers, timeout=PREMIUM_TIMEOUT_S,
                )
            else:
                r = requests.post(
                    SCRAPINGANT_ENDPOINT, params=params,
                    headers=ant_headers, json=body, timeout=PREMIUM_TIMEOUT_S,
                )
        except Exception as e:
            return {"success": False, "error": f"scrapingant: {e}"}

    return _premium_response(r, key, "scrapingant")


def _call_scraperapi(method: str, url: str, headers: dict, body=None,
                    session=None, key_label=None) -> dict:
    if key_label:
        key = _get_key_by_label("scraperapi", key_label)
    else:
        key = _pick_premium_key("scraperapi")
    if not key:
        return {"success": False, "error": "scraperapi: no live keys"}

    params = {
        "api_key":      key["key"],
        "url":          url,
        "keep_headers": "true",
    }
    if session:
        params["session_number"] = str(session)

    try:
        if method.upper() == "GET":
            r = requests.get(
                SCRAPERAPI_ENDPOINT, params=params,
                headers=headers or {}, timeout=PREMIUM_TIMEOUT_S,
            )
        else:
            r = requests.post(
                SCRAPERAPI_ENDPOINT, params=params,
                headers=headers or {}, json=body, timeout=PREMIUM_TIMEOUT_S,
            )
    except Exception as e:
        return {"success": False, "error": f"scraperapi: {e}"}

    return _premium_response(r, key, "scraperapi")


def _call_scrapeops(method: str, url: str, headers: dict, body=None,
                   session=None, key_label=None) -> dict:
    if key_label:
        key = _get_key_by_label("scrapeops", key_label)
    else:
        key = _pick_premium_key("scrapeops")
    if not key:
        return {"success": False, "error": "scrapeops: no live keys"}

    params = {
        "api_key":      key["key"],
        "url":          url,
        "keep_headers": "true",
    }
    if session:
        params["session_number"] = str(session)

    try:
        if method.upper() == "GET":
            r = requests.get(
                SCRAPEOPS_ENDPOINT, params=params,
                headers=headers or {}, timeout=PREMIUM_TIMEOUT_S,
            )
        else:
            r = requests.post(
                SCRAPEOPS_ENDPOINT, params=params,
                headers=headers or {}, json=body, timeout=PREMIUM_TIMEOUT_S,
            )
    except Exception as e:
        return {"success": False, "error": f"scrapeops: {e}"}

    return _premium_response(r, key, "scrapeops")


_PROVIDER_CALLS = {
    "scrapingant": _call_scrapingant,
    "scraperapi":  _call_scraperapi,
    "scrapeops":   _call_scrapeops,
}


# ─── Provider credit fetchers ────────────────────────────────────────────────

def _parse_iso_epoch(s) -> float:
    if not s:
        return 0.0
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _fetch_scrapingant_credits(api_key: str) -> tuple[int, int, float]:
    r = requests.get(
        SCRAPINGANT_USAGE_URL,
        params={"x-api-key": api_key},
        timeout=USAGE_TIMEOUT_S,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    total = int(j.get("plan_total_credits") or 0)
    rem   = int(j.get("remained_credits") or 0)
    used  = max(0, total - rem)
    reset = _parse_iso_epoch(j.get("end_date"))
    return total, used, reset


def _fetch_scraperapi_credits(api_key: str) -> tuple[int, int, float]:
    r = requests.get(
        SCRAPERAPI_USAGE_URL,
        params={"api_key": api_key},
        timeout=USAGE_TIMEOUT_S,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    limit = int(j.get("requestLimit") or 0)
    used  = int(j.get("requestCount") or 0)
    reset = _parse_iso_epoch(j.get("nextBillingDate"))
    return limit, used, reset


def _fetch_scrapeops_credits(api_key: str) -> tuple[int, int, float]:
    r = requests.get(
        SCRAPEOPS_USAGE_URL,
        params={"api_key": api_key},
        timeout=USAGE_TIMEOUT_S,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    inner = j.get("results") or j

    def _int(v) -> int:
        try:
            return int(float(str(v)))
        except Exception:
            return 0

    limit = _int(inner.get("plan_api_credits"))
    used  = _int(inner.get("used_api_credits"))
    reset = _parse_iso_epoch(inner.get("plan_renewal_date"))
    return limit, used, reset


_USAGE_FETCHERS = {
    "scrapingant": _fetch_scrapingant_credits,
    "scraperapi":  _fetch_scraperapi_credits,
    "scrapeops":   _fetch_scrapeops_credits,
}


def _sync_one_key(key_dict: dict) -> dict:
    provider = key_dict.get("provider", "")
    fn = _USAGE_FETCHERS.get(provider)
    if not fn:
        return {
            "label":    key_dict.get("label"),
            "provider": provider,
            "ok":       False,
            "error":    f"no usage fetcher for {provider}",
        }
    try:
        limit, used, reset = fn(key_dict.get("key", ""))
    except Exception as e:
        return {
            "label":    key_dict.get("label"),
            "provider": provider,
            "ok":       False,
            "error":    str(e),
        }
    with _keys_lock:
        key_dict["credits_limit"] = limit
        key_dict["credits_used"]  = used
        key_dict["reset_at"]      = reset
        key_dict["credit_source"] = "api"
        key_dict["last_sync"]     = time.time()
    return {
        "label":         key_dict.get("label"),
        "provider":      provider,
        "ok":            True,
        "credits_limit": limit,
        "credits_used":  used,
        "reset_at":      reset,
    }


def _sync_all_keys() -> dict:
    with _keys_lock:
        snapshot = list(_keys)
    results = []
    for k in snapshot:
        results.append(_sync_one_key(k))
    _save_keys()
    ok_count   = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    return {
        "ok":      ok_count,
        "failed":  fail_count,
        "total":   len(results),
        "results": results,
    }


def _provider_priority(plan: str) -> list[str]:
    if plan == "sticky":
        return ["scraperapi", "scrapeops"]
    if plan == "write":
        return ["scrapingant", "scraperapi", "scrapeops"]
    return ["scraperapi", "scrapeops", "scrapingant"]


def _provider_call_sync(provider: str, method: str, url: str,
                       headers: dict, body, session, key_label=None) -> dict:
    fn = _PROVIDER_CALLS.get(provider)
    if not fn:
        return {"success": False, "error": f"unknown provider {provider}"}
    return fn(method, url, headers, body, session=session, key_label=key_label)


def _derive_sticky_id(req) -> int:
    headers = req.headers or {}
    low = {str(k).lower(): v for k, v in headers.items()}
    sid = low.get("app-session-id") or low.get("instance-id") or ""
    if not sid:
        sid = req.url + json.dumps(req.body or {}, sort_keys=True)
    h = hashlib.md5(sid.encode()).hexdigest()
    return int(h[:8], 16) % 1_000_000


def _sticky_pin(flow_key: int) -> Optional[dict]:
    now = time.time()
    with _sticky_lock:
        entry = _sticky_sessions.get(str(flow_key))
        if entry and entry.get("expires_at", 0) > now:
            entry["expires_at"] = now + STICKY_TTL_S
            return entry

    for provider in ("scraperapi", "scrapeops"):
        key = _pick_premium_key(provider)
        if key:
            entry = {
                "provider":       provider,
                "key_label":      key.get("label"),
                "session_number": flow_key,
                "expires_at":     now + STICKY_TTL_S,
            }
            with _sticky_lock:
                _sticky_sessions[str(flow_key)] = entry
            return entry
    return None


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
        _reset_cycle()

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
    window_start = time.time()
    now = time.time()

    with _proxies_lock:
        items = list(_proxies.items())

    if not items:
        return []

    fresh = []
    for addr, rec in items:
        checked_at = rec.get("last_checked", 0)
        latency_s = rec.get("latency_ms", 0) / 1000.0

        if window_start - 30 <= checked_at <= now and 0 < latency_s < RACE_FRESH_MAX_S:
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
        if ROLE == "pool":
            _save_free()
        elif ROLE == "vps":
            _save_keys()

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
            timeout=6,
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
    _load_keys()
    threading.Thread(target=_pinger,         daemon=True, name="pinger").start()
    threading.Thread(target=_persist_worker, daemon=True, name="persist").start()
    print(f"[vps] started — {len(_keys)} keys loaded", flush=True)

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

def _save_keys():
    try:
        with _keys_lock:
            keys = [dict(k) for k in _keys]
        KEYS_FILE.write_text(json.dumps(keys, indent=2))
    except Exception as e:
        print(f"[persist] keys save failed: {e}", flush=True)

def _load_keys():
    if not KEYS_FILE.exists():
        return
    try:
        data = json.loads(KEYS_FILE.read_text())
        with _keys_lock:
            for entry in data:
                if "credits_used" not in entry:
                    entry["credits_used"] = 0
                if "credits_limit" not in entry:
                    entry["credits_limit"] = DEFAULT_CREDIT_LIMITS.get(
                        entry.get("provider", ""), 0)
                if "reset_at" not in entry:
                    entry["reset_at"] = 0
                if "last_used" not in entry:
                    entry["last_used"] = 0
                if "credit_source" not in entry:
                    entry["credit_source"] = "local"
                if "last_sync" not in entry:
                    entry["last_sync"] = 0
                _keys.append(entry)
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

    providers_block = {}
    with _keys_lock:
        keys_snapshot = [dict(k) for k in _keys]
    for k in keys_snapshot:
        prov  = k.get("provider", "unknown")
        limit = int(k.get("credits_limit")
                    or DEFAULT_CREDIT_LIMITS.get(prov, 0))
        used  = int(k.get("credits_used") or 0)
        block = providers_block.setdefault(prov, {
            "keys": 0, "live": 0, "total_credits": 0, "used_credits": 0,
        })
        block["keys"]          += 1
        block["total_credits"] += limit
        block["used_credits"]  += used
        if used < limit:
            block["live"] += 1

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
        "providers": providers_block,
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

# ── Pool pick-many ────────────────────────────────────────────────────────────
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

# ── Pool dead ─────────────────────────────────────────────────────────────────
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

@app.post("/reset-cycle")
async def reset_cycle_route(x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    _reset_cycle()
    return {"ok": True, "reset_at": time.time()}

# ═══════════════════════════════════════════════════════════════════════════════
# VPS /request — read/write/sticky dispatch + premium fallback
# ═══════════════════════════════════════════════════════════════════════════════
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


async def _do_free_race(req: ProxyRequest, loop) -> Optional[Response]:
    method = req.method.upper()
    proxies = await loop.run_in_executor(
        _backend_executor,
        _get_race_proxies_from_pool,
        RACE_PROXY_COUNT,
    )
    if not proxies:
        return None

    connector = aiohttp.TCPConnector(
        limit=len(proxies) + 5,
        ssl=False,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
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
                    timeout=aiohttp.ClientTimeout(total=RACE_WAIT_S),
                ) as r:
                    raw = await r.read()
                    enc = r.headers.get("Content-Encoding", "")
                    ct  = r.headers.get("Content-Type", "application/octet-stream")
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
                        _backend_executor, _report_dead_to_pool, addr,
                    )
            except Exception:
                pass
            return None

        tasks = [asyncio.create_task(_one_shot(prx)) for prx in proxies]

        try:
            for completed in asyncio.as_completed(tasks, timeout=RACE_WAIT_S + 0.2):
                result = await completed
                if result is None:
                    continue
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                _inc("race_wins")
                _log("race_win",
                     f"url={req.url} proxy={result['addr']} "
                     f"status={result['status']} candidates={len(proxies)}")
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
            await asyncio.gather(*tasks, return_exceptions=True)

    _inc("race_all_failed")
    return None


async def _do_free_single(req: ProxyRequest, loop) -> Response:
    """Single free proxy, no race, no premium fallback. FOD shot path."""
    prx = await loop.run_in_executor(
        _backend_executor, _get_proxy_from_pool, "any", "fod"
    )
    if not prx:
        _inc("free_single_no_pool")
        raise HTTPException(503, "no free proxies available")

    proxy_url = prx.get("http", "")
    if not proxy_url:
        _inc("free_single_bad_proxy")
        raise HTTPException(503, "invalid free proxy")

    addr   = prx.get("addr", "")
    method = req.method.upper()

    connector = aiohttp.TCPConnector(ssl=False, enable_cleanup_closed=True)

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.request(
                method=method,
                url=req.url,
                headers=req.headers or {},
                json=req.body if method in ("POST", "PUT", "PATCH") else None,
                params=req.params,
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=req.timeout or 10),
            ) as r:
                raw = await r.read()
                enc = r.headers.get("Content-Encoding", "")
                ct  = r.headers.get("Content-Type", "application/octet-stream")
                try:
                    if "gzip" in enc:
                        raw = gzip.decompress(raw)
                    elif "deflate" in enc:
                        raw = zlib.decompress(raw)
                except Exception:
                    pass
                _inc("free_single_ok")
                _log("free_single_ok",
                     f"url={req.url} proxy={addr} status={r.status}")
                return Response(content=raw, status_code=r.status, media_type=ct)
    except asyncio.TimeoutError:
        _inc("free_single_timeout")
        if addr:
            await loop.run_in_executor(_backend_executor, _report_dead_to_pool, addr)
        raise HTTPException(504, "free single timeout")
    except (aiohttp.ClientProxyConnectionError, aiohttp.ClientConnectionError):
        _inc("free_single_conn_err")
        if addr:
            await loop.run_in_executor(_backend_executor, _report_dead_to_pool, addr)
        raise HTTPException(502, "free proxy connection failed")
    except Exception as e:
        _inc("free_single_err")
        raise HTTPException(502, f"free single failed: {e}")


async def _premium_forward(req: ProxyRequest, plan: str, loop) -> Response:
    method   = req.method.upper()
    providers = _provider_priority(plan)
    sticky_entry = None

    if plan == "sticky":
        flow_key = _derive_sticky_id(req)
        sticky_entry = await loop.run_in_executor(_backend_executor, _sticky_pin, flow_key)
        if not sticky_entry:
            raise HTTPException(503, "no premium provider available for sticky flow")
        providers = [sticky_entry["provider"]]

    last_err = None
    for provider in providers:
        pinned_label = sticky_entry.get("key_label") if sticky_entry else None
        session_id   = sticky_entry.get("session_number") if sticky_entry else None

        result = await loop.run_in_executor(
            _backend_executor,
            _provider_call_sync,
            provider, method, req.url, req.headers or {},
            req.body if method in ("POST", "PUT", "PATCH") else None,
            session_id, pinned_label,
        )

        if result.get("success"):
            data = result.get("data")
            if isinstance(data, (dict, list)):
                raw = json.dumps(data).encode()
                ct  = "application/json"
            else:
                raw = str(data).encode()
                ct  = "text/plain"
            _inc(f"premium_ok_{provider}")
            _log("premium_ok", f"url={req.url} provider={provider} plan={plan}")
            return Response(content=raw, status_code=200, media_type=ct)

        last_err = result.get("error", "unknown")
        _inc(f"premium_fail_{provider}")
        _log("premium_fail", f"url={req.url} provider={provider} err={last_err}")

    raise HTTPException(502, f"all premium providers failed: {last_err}")


@app.post("/request")
async def proxy_request(req: ProxyRequest):
    if ROLE != "vps":
        raise HTTPException(400, "/request only on vps role")

    loop = asyncio.get_event_loop()

    # ─── Tier overrides (win over URL planning) ─────────────────────────────
    if req.tier == "premium":
        _inc("requests_premium_forced")
        return await _premium_forward(req, "read", loop)

    if req.tier == "free_only":
        _inc("requests_free_only")
        return await _do_free_single(req, loop)

    if req.tier == "paid":
        _inc("requests_premium_forced")
        return await _premium_forward(req, "read", loop)

    # FOD URLs with tier="any" default to free_only — never race, never burn
    # premium. Index.py always sends explicit tier, this is a safety net.
    if _is_fod_url(req.url) and req.tier == "any":
        _inc("requests_fod_default_free")
        return await _do_free_single(req, loop)

    # ─── URL-based planning ─────────────────────────────────────────────────
    plan = _plan_for_url(req.url)

    if plan == "read":
        _inc("requests_read")
        result = await _do_free_race(req, loop)
        if result is not None:
            return result
        return await _premium_forward(req, "read", loop)

    if plan == "write":
        _inc("requests_write")
        return await _premium_forward(req, "write", loop)

    if plan == "sticky":
        _inc("requests_sticky")
        return await _premium_forward(req, "sticky", loop)

    raise HTTPException(500, "unknown plan")


# ═══════════════════════════════════════════════════════════════════════════════
# VPS KEY MANAGEMENT — VPS handles, dashboard forwards
# ═══════════════════════════════════════════════════════════════════════════════
class AddKeyRequest(BaseModel):
    provider:      str
    key:           str
    label:         Optional[str] = ""
    credits_limit: Optional[int] = None

class RemoveKeyRequest(BaseModel):
    key:   Optional[str] = None
    label: Optional[str] = None


def _vps_forward(method: str, path: str, body=None) -> tuple[int, dict]:
    base = PEERS.get("vps", "").rstrip("/")
    if not base:
        return 503, {"error": "vps not configured"}
    try:
        r = requests.request(
            method, f"{base}{path}",
            json=body,
            headers={"X-Secret": SHARED_SECRET},
            timeout=15,
        )
        try:
            data = r.json() if r.content else {}
        except Exception:
            data = {"raw": r.text[:300]}
        return r.status_code, data
    except Exception as e:
        return 502, {"error": str(e)}


@app.post("/keys/add")
async def add_key(req: AddKeyRequest):
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/add", req.dict(),
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    provider = req.provider.strip().lower()
    if provider not in PROVIDERS:
        raise HTTPException(400, f"provider must be one of {PROVIDERS}")

    auto_label = req.label or f"{provider}-{req.key[:8]}"
    with _keys_lock:
        if any(k["key"] == req.key for k in _keys):
            raise HTTPException(400, "key already exists")
        existing = {k["label"] for k in _keys}
        final    = auto_label
        suffix   = 2
        while final in existing:
            final = f"{auto_label}-{suffix}"
            suffix += 1
        entry = {
            "provider":      provider,
            "key":           req.key,
            "label":         final,
            "credits_limit": req.credits_limit or DEFAULT_CREDIT_LIMITS.get(provider, 1000),
            "credits_used":  0,
            "reset_at":      0,
            "last_used":     0,
            "credit_source": "local",
            "last_sync":     0,
        }
        _keys.append(entry)
    _save_keys()
    return {"status": "ok", "label": final, "provider": provider}


@app.delete("/keys/remove")
async def remove_key(req: RemoveKeyRequest):
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "DELETE", "/keys/remove", req.dict(),
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    with _keys_lock:
        if req.key:
            entry = next((k for k in _keys if k["key"] == req.key), None)
        elif req.label:
            entry = next((k for k in _keys if k["label"] == req.label), None)
        else:
            raise HTTPException(400, "key or label required")
        if not entry:
            raise HTTPException(404, "key not found")
        label    = entry["label"]
        _keys[:] = [k for k in _keys if k["label"] != label]
    _save_keys()
    return {"status": "ok", "removed": label}


@app.get("/keys/list")
async def list_keys():
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "GET", "/keys/list",
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    with _keys_lock:
        return [
            {
                "label":    k["label"],
                "provider": k["provider"],
                "hint":     f"***{k['key'][-6:]}" if k.get("key") else "",
            }
            for k in _keys
        ]


@app.get("/keys/stats")
async def keys_stats():
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "GET", "/keys/stats",
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    with _keys_lock:
        keys = [dict(k) for k in _keys]

    by_provider: dict[str, dict] = defaultdict(
        lambda: {"keys": 0, "live": 0, "total_credits": 0, "used_credits": 0}
    )
    out_keys = []
    for k in keys:
        prov  = k.get("provider", "")
        limit = int(k.get("credits_limit")
                    or DEFAULT_CREDIT_LIMITS.get(prov, 0))
        used  = int(k.get("credits_used") or 0)
        live  = used < limit
        out_keys.append({
            "label":         k.get("label"),
            "provider":      prov,
            "credits_limit": limit,
            "credits_used":  used,
            "live":          live,
            "last_used":     k.get("last_used", 0),
            "credit_source": k.get("credit_source", "local"),
            "last_sync":     k.get("last_sync", 0),
            "hint":          f"***{k['key'][-6:]}" if k.get("key") else "",
        })
        block = by_provider[prov]
        block["keys"]          += 1
        block["total_credits"] += limit
        block["used_credits"]  += used
        if live:
            block["live"] += 1

    return {"providers": dict(by_provider), "keys": out_keys}


@app.post("/keys/sync")
async def keys_sync(x_secret: Optional[str] = Header(None)):
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/sync",
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_api_executor, _sync_all_keys)
    return result


@app.post("/keys/reset-credits")
async def keys_reset_credits(x_secret: Optional[str] = Header(None)):
    if ROLE == "dashboard":
        loop = asyncio.get_event_loop()
        code, body = await loop.run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/reset-credits",
        )
        if code >= 400:
            raise HTTPException(code, body.get("detail", body.get("error", "vps error")))
        return body

    if x_secret != SHARED_SECRET:
        raise HTTPException(403, "invalid secret")
    if ROLE != "vps":
        raise HTTPException(400, "keys only on vps or dashboard")

    with _keys_lock:
        for k in _keys:
            k["credits_used"] = 0
            k["reset_at"]     = 0
            k["credit_source"] = "local"
    _save_keys()
    return {"status": "ok", "reset": len(_keys)}


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