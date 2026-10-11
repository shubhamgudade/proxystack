"""
ProxyStack v2 — Distributed Proxy Gateway with Weighted Balancing
===================================================================
ROLES:
  checker_1..10 → fetch proxies, check meesho, push to pool
  pool          → ingest, pick, dead, recheck, persist, SSE
  dashboard     → stats aggregator + dashboard UI
  vps           → /request, premium providers, keys

NEW in this version:
  • ntfy.sh push notifications (key 90%, exhausted, provider down, fallback)
  • Weighted load balancing by remaining credits per provider
  • Per-endpoint timeouts (FOD 60s, referral 45s, config 30s)
  • usage_check key role — dedicated key for /keys/sync, never does work
  • renewal tag per key (renewable / non_renewable)
  • reset_at_override for providers without reset API (ZenRows, Scrape.do)
  • Auto-roll reset_at by reset_interval_days when override passes
  • Dead keys removed, new Scrape.do / ZenRows / WebScraping.AI keys added
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
# ROLE + CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
ROLE          = os.getenv("ROLE", "pool").lower()
SHARED_SECRET = os.getenv("SHARED_SECRET", "changeme")
PORT          = int(os.getenv("PORT", "8080"))
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "")

# ntfy.sh push notifications — zero signup, free, unlimited
NTFY_TOPIC    = os.getenv("NTFY_TOPIC", "")   # set to topic name to enable
NTFY_BASE     = os.getenv("NTFY_BASE", "https://ntfy.sh")
NTFY_ENABLED  = bool(NTFY_TOPIC)

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
CALLER_PORT_MAP = {
    8001: "MesoWebBackend",
    8090: "pinger_dashboard",
    8080: "proxystack-internal",
}

def _identify_caller(request: Request) -> str:
    if request.client is None:
        return "unknown"
    return CALLER_PORT_MAP.get(request.client.port, f"unknown:{request.client.port}")

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

# Per-endpoint timeout overrides (FOD/referral are slow)
TIMEOUT_BY_PATH = {
    "/fod-personalisation":    60,
    "/referral-app-install":   45,
    "/anonymous/config":       30,
}
DEFAULT_PREMIUM_TIMEOUT = 30
USAGE_TIMEOUT_S = 10
APIFY_TIMEOUT_S = 120

# Notification thresholds
NOTIFY_90_PCT     = 0.90
NOTIFY_75_PCT     = 0.75
PROVIDER_COOLDOWN_S     = 45
PROVIDER_FAIL_THRESHOLD = 2

# ═══════════════════════════════════════════════════════════════════════════════
# URL ROUTING
# ═══════════════════════════════════════════════════════════════════════════════
RACE_WAIT_S      = 3.0
RACE_FRESH_MAX_S = 4.0
RACE_PROXY_COUNT = 6

STICKY_URL_FRAGMENTS = ["/api/2.0/user/login", "/api/4.0/preorders"]
STICKY_EXACT_PATHS   = ["/api/3.0/order"]

WRITE_URL_FRAGMENTS = [
    "/api/1.0/cart/add", "/api/1.0/cart/remove", "/api/1.0/cart/location",
    "/api/2.0/addresses", "/api/1.0/user/delivery-location", "/api/2.0/orders/",
]

RACEABLE_URL_FRAGMENTS = [
    "/api/3.0/search/suggest", "/api/3.0/anonymous/search/suggest",
    "/api/1.0/widget-groups/fetch", "/api/1.0/anonymous/widget-groups/fetch",
    "/api/3.0/product/static", "/api/3.0/product/dynamic",
    "/api/2.0/catalogs/", "/api/2.0/anonymous/catalogs/",
    "/api/1.0/catalogs/recommendations", "/api/1.0/anonymous/catalogs/recommendations",
    "/api/4.0/anonymous/for-you", "/api/3.0/user/orders",
    "/api/3.0/user/order-details", "/api/1.0/cart/paymentinfo",
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

def _timeout_for_url(url: str) -> int:
    path = _url_path(url)
    for frag, t in TIMEOUT_BY_PATH.items():
        if frag in path:
            return t
    return DEFAULT_PREMIUM_TIMEOUT

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCES
# ═══════════════════════════════════════════════════════════════════════════════
ALL_GITHUB_REPOS = [
    ("monosans","proxy-list","proxies/http.txt"),
    ("monosans","proxy-list","proxies_anonymous/http.txt"),
    ("TheSpeedX","PROXY-List","http.txt"),
    ("proxifly","free-proxy-list","proxies/protocols/http/data.txt"),
    ("proxifly","free-proxy-list","proxies/all/data.txt"),
    ("roosterkid","openproxylist","HTTPS_RAW.txt"),
    ("mmpx12","proxy-list","http.txt"),
    ("clarketm","proxy-list","proxy-list-raw.txt"),
    ("ShiftyTR","Proxy-List","http.txt"),
    ("rdavydov","proxy-list","proxies/http.txt"),
    ("rdavydov","proxy-list","proxies_anonymous/http.txt"),
    ("zevtyardt","proxy-list","http.txt"),
    ("proxy4parsing","proxy-list","http.txt"),
    ("sunny9577","proxy-scraper","proxies.txt"),
    ("zloi-user","hideip.me","http.txt"),
    ("bq2015","FreeProxies","http.txt"),
    ("Ian-Lusule","Proxies","http.txt"),
    ("fate0","proxylist","proxy.list"),
    ("jetkai","proxy-list","online-proxies/txt/proxies-http.txt"),
    ("HyperBeats","Free-Proxies","http.txt"),
    ("elliottophellia","proxylist","http/HTTP_ALL.txt"),
    ("prxchk","proxy-list","http.txt"),
    ("Anonym0usWork1221","free-proxy-list","free-proxy/http.txt"),
    ("officialputuid","open-proxy-scrapper","lists/http.txt"),
    ("ErcinDedeoglu","proxylists","lists/http.txt"),
    ("ObcbO","free-proxy","http.txt"),
    ("Vann-Dev","free-proxy-list","proxy-list/http.txt"),
    ("almroot","proxylist","list.txt"),
    ("aslisk","public_socks5_lists","http.txt"),
    ("MuRongPIG","Free-Proxies","TheBiggerPicture.txt"),
    ("casals","hma-proxy-list","http.txt"),
    ("BlackSnowDot","proxy-list","http-proxies.txt"),
    ("im-razvan","http-proxy","http.txt"),
    ("UserR00T","Proxy-List","Proxylists/http.txt"),
    ("UptimerBot","proxy-list","proxies/http.txt"),
    ("yuceltoluyag","socks4-socks5-http-proxylist","http.txt"),
    ("TundzhaSarl","free-proxy-list","lists/http.txt"),
    ("mertguvencli","http-proxy-list","list.txt"),
    ("proxylist2022","proxy-list","http.txt"),
    ("saisuiu","free-public-proxy","http.txt"),
    ("Zaeem20","free-proxies-scraper","Proxies/Http/http-proxies.txt"),
    ("roma8ka","free-proxy-list","http.txt"),
    ("MathiasMillingdale","free-http-proxy-list","http.txt"),
    ("Fkunn1326","opz-proxies","http.txt"),
    ("hookzof","socks5_list","proxy/socks5.txt"),
    ("calpt","glitch-socks","http.txt"),
    ("iplocate","free-proxy-list","protocols/http.txt"),
    ("adasd223","checked-proxy-list","http.txt"),
    ("gfpcom","free-proxy-list","lists/http.txt"),
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
    "https://free-proxy-list.net","https://sslproxies.org",
    "https://us-proxy.org","https://free-proxy-list.net/anonymous-proxy.html",
]

def _split_sources(lst, n, idx):
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
# NOTIFICATIONS (ntfy.sh)
# ═══════════════════════════════════════════════════════════════════════════════
_notified_90pct:        set = set()
_notified_exhausted:    set = set()
_notified_provider_down: set = set()
_notified_lock = threading.Lock()

def notify(title: str, message: str, priority: str = "default",
           tags: Optional[list] = None):
    """Fire-and-forget push via ntfy.sh. No-ops if NTFY_TOPIC not set."""
    if not NTFY_ENABLED:
        return
    try:
        headers = {
            "Title":    title,
            "Priority": priority,
            "Tags":     ",".join(tags or ["warning"]),
        }
        requests.post(
            f"{NTFY_BASE}/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=5,
        )
    except Exception as e:
        print(f"[notify] failed: {e}", flush=True)

def _notify_once(tracker: set, key: str, title: str, msg: str,
                 priority: str, tags: list):
    with _notified_lock:
        if key in tracker:
            return
        tracker.add(key)
    notify(title, msg, priority, tags)

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
_snap_fresh30:  list = []
_snap_fresh120: list = []
_snap_fast:     list = []

_keys_lock = threading.Lock()
_keys: list = []

_key_sems_lock = threading.Lock()
_key_sems: dict = {}

def _get_key_sem(key_dict: dict) -> threading.Semaphore:
    label    = key_dict.get("label", "unknown")
    provider = key_dict.get("provider", "")
    with _key_sems_lock:
        if label not in _key_sems:
            _key_sems[label] = threading.Semaphore(
                KEY_CONCURRENCY_LIMITS.get(provider, 1)
            )
        return _key_sems[label]

_sticky_lock = threading.Lock()
_sticky_sessions: dict = {}

_seen_lock = threading.Lock()
_seen: dict = {}

_outbuf_lock = threading.Lock()
_outbuf: list = []

_counter_lock = threading.Lock()
_counters_lifetime: dict = defaultdict(int)
_counters_cycle:    dict = defaultdict(int)
_cycle_start: float = time.time()

_activity_lock = threading.Lock()
_activity_log: list = []
ACTIVITY_MAX = 200

_persist_event = threading.Event()

_sha_lock  = threading.Lock()
_repo_sha: dict = {}

_etag_lock = threading.Lock()
_http_etag: dict = {}
_http_lmod: dict = {}

_provider_cooldown_lock = threading.Lock()
_provider_cooldown:      dict = {}
_provider_fail_streak:   dict = {}

_check_queue = None
_check_loop  = None

_api_executor     = ThreadPoolExecutor(max_workers=8,  thread_name_prefix="api")
_backend_executor = ThreadPoolExecutor(max_workers=24, thread_name_prefix="backend")

# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════
SCRAPINGANT_ENDPOINT  = "https://api.scrapingant.com/v2/general"
SCRAPERAPI_ENDPOINT   = "https://api.scraperapi.com/"
SCRAPEOPS_ENDPOINT    = "https://proxy.scrapeops.io/v1/"
SCRAPINGBEE_ENDPOINT  = "https://app.scrapingbee.com/api/v1/"
SCRAPFLY_ENDPOINT     = "https://api.scrapfly.io/scrape"
SCRAPEDO_ENDPOINT     = "http://api.scrape.do"
ZENROWS_ENDPOINT      = "https://api.zenrows.com/v1/"
WEBSCRAPINGAI_ENDPOINT= "https://api.webscraping.ai/html"
APIFY_ENDPOINT        = ("https://api.apify.com/v2/acts/"
                         "parsebird~http-request-actor/run-sync-get-dataset-items")

SCRAPINGANT_USAGE_URL   = "https://api.scrapingant.com/v2/usage"
SCRAPERAPI_USAGE_URL    = "https://api.scraperapi.com/account"
SCRAPEOPS_USAGE_URL     = "https://backend.scrapeops.io/v1/proxy/account/usage"
SCRAPINGBEE_USAGE_URL   = "https://app.scrapingbee.com/api/v1/usage"
SCRAPFLY_ACCOUNT_URL    = "https://api.scrapfly.io/account"
SCRAPEDO_USAGE_URL      = "https://api.scrape.do/info"
WEBSCRAPINGAI_USAGE_URL = "https://api.webscraping.ai/account"
APIFY_USAGE_URL         = "https://api.apify.com/v2/users/me/usage/monthly"

# ═══════════════════════════════════════════════════════════════════════════════
# PROVIDER CALLS
# ═══════════════════════════════════════════════════════════════════════════════

def _premium_response(r: requests.Response, key: dict, provider: str) -> dict:
    if r.status_code in (401, 403, 429):
        _exhaust_key(key, reason=f"http {r.status_code}")
        return {"success": False, "error": f"{provider}: http {r.status_code}",
                "rotate": True}
    _mark_key_used(key, credits=1)
    if r.status_code >= 400:
        return {"success": False, "error": f"{provider}: http {r.status_code}",
                "raw": r.text[:300]}
    try:
        data = r.json()
    except Exception:
        data = r.text
    return {"success": True, "data": data}

def _premium_response_text(r: requests.Response, key: dict, provider: str) -> dict:
    if r.status_code in (401, 403, 429):
        _exhaust_key(key, reason=f"http {r.status_code}")
        return {"success": False, "error": f"{provider}: http {r.status_code}",
                "rotate": True}
    _mark_key_used(key, credits=1)
    if r.status_code >= 400:
        return {"success": False, "error": f"{provider}: http {r.status_code}",
                "raw": r.text[:300]}
    text = r.text.strip()
    try:
        data = json.loads(text)
    except Exception:
        data = text
    return {"success": True, "data": data}

# ── ScrapingAnt (custom headers via Ant- prefix) ─────────────────────────────
def _call_scrapingant(method, url, headers, body=None, session=None,
                      key_label=None, timeout=None):
    key = _get_key_by_label("scrapingant", key_label) if key_label else _pick_premium_key("scrapingant")
    if not key: return {"success": False, "error": "scrapingant: no live keys"}
    sem = _get_key_sem(key)
    ant_headers = {"x-api-key": key["key"]}
    for k, v in (headers or {}).items():
        ant_headers[f"ant-{k}"] = v
    params = {"url": url, "browser": "false", "return_page_source": "true"}
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPINGANT_ENDPOINT, params=params,
                                 headers=ant_headers, timeout=tmo)
            else:
                r = requests.post(SCRAPINGANT_ENDPOINT, params=params,
                                  headers=ant_headers, json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scrapingant: {e}"}
    return _premium_response(r, key, "scrapingant")

# ── ScraperAPI ────────────────────────────────────────────────────────────────
def _call_scraperapi(method, url, headers, body=None, session=None,
                     key_label=None, timeout=None):
    key = _get_key_by_label("scraperapi", key_label) if key_label else _pick_premium_key("scraperapi")
    if not key: return {"success": False, "error": "scraperapi: no live keys"}
    sem = _get_key_sem(key)
    params = {"api_key": key["key"], "url": url, "keep_headers": "true"}
    if session: params["session_number"] = str(session)
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPERAPI_ENDPOINT, params=params,
                                 headers=headers or {}, timeout=tmo)
            else:
                r = requests.post(SCRAPERAPI_ENDPOINT, params=params,
                                  headers=headers or {}, json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scraperapi: {e}"}
    return _premium_response(r, key, "scraperapi")

# ── ScrapeOps ─────────────────────────────────────────────────────────────────
def _call_scrapeops(method, url, headers, body=None, session=None,
                    key_label=None, timeout=None):
    key = _get_key_by_label("scrapeops", key_label) if key_label else _pick_premium_key("scrapeops")
    if not key: return {"success": False, "error": "scrapeops: no live keys"}
    sem = _get_key_sem(key)
    params = {"api_key": key["key"], "url": url, "keep_headers": "true"}
    if session: params["session_number"] = str(session)
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPEOPS_ENDPOINT, params=params,
                                 headers=headers or {}, timeout=tmo)
            else:
                r = requests.post(SCRAPEOPS_ENDPOINT, params=params,
                                  headers=headers or {}, json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scrapeops: {e}"}
    return _premium_response(r, key, "scrapeops")

# ── ScrapingBee ───────────────────────────────────────────────────────────────
def _call_scrapingbee(method, url, headers, body=None, session=None,
                      key_label=None, timeout=None):
    key = _get_key_by_label("scrapingbee", key_label) if key_label else _pick_premium_key("scrapingbee")
    if not key: return {"success": False, "error": "scrapingbee: no live keys"}
    sem = _get_key_sem(key)
    params = {"api_key": key["key"], "url": url,
              "render_js": "false", "forward_headers": "true"}
    spb_headers = {f"Spb-{k}": v for k, v in (headers or {}).items()}
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPINGBEE_ENDPOINT, params=params,
                                 headers=spb_headers, timeout=tmo)
            else:
                r = requests.post(SCRAPINGBEE_ENDPOINT, params=params,
                                  headers=spb_headers,
                                  data=json.dumps(body) if body else None,
                                  timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scrapingbee: {e}"}
    return _premium_response_text(r, key, "scrapingbee")

# ── Scrapfly ──────────────────────────────────────────────────────────────────
def _call_scrapfly(method, url, headers, body=None, session=None,
                   key_label=None, timeout=None):
    key = _get_key_by_label("scrapfly", key_label) if key_label else _pick_premium_key("scrapfly")
    if not key: return {"success": False, "error": "scrapfly: no live keys"}
    sem = _get_key_sem(key)
    params = [("key", key["key"]), ("url", url),
              ("asp", "false"), ("render_js", "false")]
    for k, v in (headers or {}).items():
        params.append((f"headers[{k}]", str(v)))
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPFLY_ENDPOINT, params=params, timeout=tmo)
            else:
                r = requests.post(SCRAPFLY_ENDPOINT, params=params,
                                  data=json.dumps(body) if body else None,
                                  headers={"Content-Type": "application/json"},
                                  timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scrapfly: {e}"}
    if r.status_code in (401, 403, 429):
        _exhaust_key(key, reason=f"http {r.status_code}")
        return {"success": False, "error": f"scrapfly: http {r.status_code}", "rotate": True}
    _mark_key_used(key, credits=1)
    if r.status_code >= 400:
        return {"success": False, "error": f"scrapfly: http {r.status_code}",
                "raw": r.text[:300]}
    try:
        outer = r.json()
        inner = outer.get("result", {}).get("content", "")
        try:
            data = json.loads(inner) if isinstance(inner, str) else inner
        except Exception:
            data = inner or outer
        return {"success": True, "data": data}
    except Exception:
        return {"success": True, "data": r.text}

# ── Scrape.do ─────────────────────────────────────────────────────────────────
def _call_scrapedo(method, url, headers, body=None, session=None,
                   key_label=None, timeout=None):
    key = _get_key_by_label("scrapedo", key_label) if key_label else _pick_premium_key("scrapedo")
    if not key: return {"success": False, "error": "scrapedo: no live keys"}
    sem = _get_key_sem(key)
    params = {"token": key["key"], "url": url, "geoCode": "in",
              "super": "false", "render": "false", "customHeaders": "true"}
    if session: params["sessionId"] = str(session)
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(SCRAPEDO_ENDPOINT, params=params,
                                 headers=headers or {}, timeout=tmo)
            else:
                r = requests.post(SCRAPEDO_ENDPOINT, params=params,
                                  headers=headers or {}, json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"scrapedo: {e}"}
    return _premium_response_text(r, key, "scrapedo")

# ── ZenRows ───────────────────────────────────────────────────────────────────
def _call_zenrows(method, url, headers, body=None, session=None,
                  key_label=None, timeout=None):
    key = _get_key_by_label("zenrows", key_label) if key_label else _pick_premium_key("zenrows")
    if not key: return {"success": False, "error": "zenrows: no live keys"}
    sem = _get_key_sem(key)
    params = {"apikey": key["key"], "url": url, "custom_headers": "true"}
    if session: params["session_id"] = str(session)
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(ZENROWS_ENDPOINT, params=params,
                                 headers=headers or {}, timeout=tmo)
            else:
                r = requests.post(ZENROWS_ENDPOINT, params=params,
                                  headers=headers or {}, json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"zenrows: {e}"}
    return _premium_response_text(r, key, "zenrows")

# ── WebScraping.AI ────────────────────────────────────────────────────────────
def _call_webscrapingai(method, url, headers, body=None, session=None,
                        key_label=None, timeout=None):
    key = _get_key_by_label("webscrapingai", key_label) if key_label else _pick_premium_key("webscrapingai")
    if not key: return {"success": False, "error": "webscrapingai: no live keys"}
    sem = _get_key_sem(key)
    params = {"api_key": key["key"], "url": url}
    for k, v in (headers or {}).items():
        if k.lower() not in ("accept-encoding", "connection", "host"):
            params[f"headers[{k}]"] = str(v)
    if session: params["session_id"] = str(session)
    tmo = timeout or DEFAULT_PREMIUM_TIMEOUT
    with sem:
        try:
            if method.upper() == "GET":
                r = requests.get(WEBSCRAPINGAI_ENDPOINT, params=params, timeout=tmo)
            else:
                r = requests.post(WEBSCRAPINGAI_ENDPOINT, params=params,
                                  json=body, timeout=tmo)
        except Exception as e:
            return {"success": False, "error": f"webscrapingai: {e}"}
    return _premium_response_text(r, key, "webscrapingai")

# ── Apify ─────────────────────────────────────────────────────────────────────
def _call_apify(method, url, headers, body=None, session=None,
                key_label=None, timeout=None):
    key = _get_key_by_label("apify", key_label) if key_label else _pick_premium_key("apify")
    if not key: return {"success": False, "error": "apify: no live keys"}
    sem = _get_key_sem(key)
    run_input = {"url": url, "method": method.upper(), "headers": headers or {}}
    if method.upper() == "POST" and body:
        run_input["data"] = body
    tmo = timeout or APIFY_TIMEOUT_S
    with sem:
        try:
            r = requests.post(
                APIFY_ENDPOINT,
                headers={"Authorization": f"Bearer {key['key']}",
                         "Content-Type": "application/json"},
                json=run_input, timeout=tmo,
            )
        except Exception as e:
            return {"success": False, "error": f"apify: {e}"}
    if r.status_code in (401, 403, 429):
        _exhaust_key(key, reason=f"http {r.status_code}")
        return {"success": False, "error": f"apify: http {r.status_code}", "rotate": True}
    _mark_key_used(key, credits=1)
    if r.status_code >= 400:
        return {"success": False, "error": f"apify: http {r.status_code}",
                "raw": r.text[:300]}
    try:
        items = r.json()
    except Exception:
        return {"success": True, "data": r.text}
    if not isinstance(items, list) or not items:
        return {"success": True, "data": items}
    item = items[0]
    if "data" in item:
        return {"success": True, "data": item["data"]}
    if "body" in item:
        status = item.get("statusCode", 0)
        if status >= 400:
            return {"success": False, "error": f"apify: target http {status}"}
        b = item.get("body", "")
        try: return {"success": True, "data": json.loads(b)}
        except Exception: return {"success": True, "data": b}
    return {"success": True, "data": item}

# ═══════════════════════════════════════════════════════════════════════════════
# USAGE FETCHERS
# ═══════════════════════════════════════════════════════════════════════════════
def _parse_iso_epoch(s) -> float:
    if not s: return 0.0
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0

def _fetch_scrapingant_credits(api_key):
    r = requests.get(SCRAPINGANT_USAGE_URL, params={"x-api-key": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    total = int(j.get("plan_total_credits") or 0)
    rem   = int(j.get("remained_credits") or 0)
    return total, max(0, total - rem), _parse_iso_epoch(j.get("end_date"))

def _fetch_scraperapi_credits(api_key):
    r = requests.get(SCRAPERAPI_USAGE_URL, params={"api_key": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    return (int(j.get("requestLimit") or 0),
            int(j.get("requestCount") or 0),
            _parse_iso_epoch(j.get("nextBillingDate")))

def _fetch_scrapeops_credits(api_key):
    r = requests.get(SCRAPEOPS_USAGE_URL, params={"api_key": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    inner = j.get("results") or j
    def _i(v):
        try: return int(float(str(v)))
        except Exception: return 0
    return (_i(inner.get("plan_api_credits")),
            _i(inner.get("used_api_credits")),
            _parse_iso_epoch(inner.get("plan_renewal_date")))

def _fetch_scrapingbee_credits(api_key):
    r = requests.get(SCRAPINGBEE_USAGE_URL, params={"api_key": api_key},
                     headers={"Accept": "application/json"},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    return (int(j.get("max_api_credit") or 0),
            int(j.get("used_api_credit") or 0),
            _parse_iso_epoch(j.get("renewal_subscription_date")))

def _fetch_scrapfly_credits(api_key):
    r = requests.get(SCRAPFLY_ACCOUNT_URL, params={"key": api_key},
                     headers={"Accept": "application/json"},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    sub    = j.get("subscription") or {}
    usage  = sub.get("usage") or {}
    scrape = usage.get("scrape") or {}
    return (int(scrape.get("limit") or 0),
            int(scrape.get("current") or 0),
            _parse_iso_epoch((sub.get("period") or {}).get("end")))

def _fetch_scrapedo_credits(api_key):
    """Scrape.do doesn't return a reset date — use override."""
    r = requests.get(SCRAPEDO_USAGE_URL, params={"token": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    total = int(j.get("MaxMonthlyRequest") or 0)
    rem   = int(j.get("RemainingMonthlyRequest") or 0)
    return total, max(0, total - rem), 0.0   # 0 → use override

def _fetch_webscrapingai_credits(api_key):
    r = requests.get(WEBSCRAPINGAI_USAGE_URL, params={"api_key": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json()
    monthly = int(j.get("remaining_monthly_credits") or 0)
    payg    = int(j.get("remaining_payg_credits") or 0)
    total_rem = int(j.get("remaining_total_credits") or (monthly + payg))
    limit = monthly + payg + int(j.get("used_credits") or 0)
    if limit == 0: limit = total_rem  # fallback
    return limit, max(0, limit - total_rem), _parse_iso_epoch(j.get("resets_at"))

def _fetch_apify_credits(api_key):
    r = requests.get(APIFY_USAGE_URL, params={"token": api_key},
                     timeout=USAGE_TIMEOUT_S)
    if r.status_code >= 400:
        raise RuntimeError(f"http {r.status_code}: {r.text[:120]}")
    j = r.json().get("data", {})
    cycle = (j.get("usageCycle") or {})
    used_usd = float(j.get("totalUsageCreditsUsdAfterVolumeDiscount") or 0)
    limit_usd = float(j.get("monthlyUsageCycle", {}).get("creditsUsd") or 5.0)
    # scale to int credits (1 credit = $0.001)
    return (int(limit_usd * 1000), int(used_usd * 1000),
            _parse_iso_epoch(cycle.get("endAt")))

_USAGE_FETCHERS = {
    "scrapingant":   _fetch_scrapingant_credits,
    "scraperapi":    _fetch_scraperapi_credits,
    "scrapeops":     _fetch_scrapeops_credits,
    "scrapingbee":   _fetch_scrapingbee_credits,
    "scrapfly":      _fetch_scrapfly_credits,
    "scrapedo":      _fetch_scrapedo_credits,
    "webscrapingai": _fetch_webscrapingai_credits,
    "apify":         _fetch_apify_credits,
    # zenrows: no public usage API
}

# ═══════════════════════════════════════════════════════════════════════════════
# PROVIDER REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════
PROVIDER_SPECS = {
    "scrapingant": {
        "call": _call_scrapingant, "usage_fetcher": _fetch_scrapingant_credits,
        "default_limit": 10_000, "concurrency": 1,
        "supports_sticky": True,  "tier": "primary",
        "renewal": "non_renewable",
    },
    "scrapeops": {
        "call": _call_scrapeops, "usage_fetcher": _fetch_scrapeops_credits,
        "default_limit": 100_000, "concurrency": 3,
        "supports_sticky": True,  "tier": "primary",
        "renewal": "renewable",
    },
    "scrapingbee": {
        "call": _call_scrapingbee, "usage_fetcher": _fetch_scrapingbee_credits,
        "default_limit": 1_000, "concurrency": 5,
        "supports_sticky": False, "tier": "primary",
        "renewal": "non_renewable",
    },
    "scrapfly": {
        "call": _call_scrapfly, "usage_fetcher": _fetch_scrapfly_credits,
        "default_limit": 1_000, "concurrency": 5,
        "supports_sticky": False, "tier": "primary",
        "renewal": "renewable",
    },
    "scrapedo": {
        "call": _call_scrapedo, "usage_fetcher": _fetch_scrapedo_credits,
        "default_limit": 1_000, "concurrency": 5,
        "supports_sticky": True,  "tier": "primary",
        "renewal": "renewable",
    },
    "zenrows": {
        "call": _call_zenrows, "usage_fetcher": None,
        "default_limit": 1_000, "concurrency": 5,
        "supports_sticky": True,  "tier": "primary",
        "renewal": "renewable",
    },
    "webscrapingai": {
        "call": _call_webscrapingai, "usage_fetcher": _fetch_webscrapingai_credits,
        "default_limit": 2_000, "concurrency": 5,
        "supports_sticky": False, "tier": "primary",
        "renewal": "renewable",
    },
    "scraperapi": {
        "call": _call_scraperapi, "usage_fetcher": _fetch_scraperapi_credits,
        "default_limit": 5_000, "concurrency": 5,
        "supports_sticky": False, "tier": "last_resort",
        "renewal": "renewable",
    },
    "apify": {
        "call": _call_apify, "usage_fetcher": _fetch_apify_credits,
        "default_limit": 5_000, "concurrency": 1,
        "supports_sticky": False, "tier": "last_resort",
        "renewal": "renewable",
    },
}

PROVIDERS              = tuple(PROVIDER_SPECS.keys())
KEY_CONCURRENCY_LIMITS = {p: s["concurrency"]    for p, s in PROVIDER_SPECS.items()}
_PROVIDER_CALLS        = {p: s["call"]           for p, s in PROVIDER_SPECS.items()}
DEFAULT_CREDIT_LIMITS  = {p: s["default_limit"]  for p, s in PROVIDER_SPECS.items()}

# ═══════════════════════════════════════════════════════════════════════════════
# KEY MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════
def _apply_reset_rollover(k: dict):
    """If reset_at_override is set and past, roll forward by interval days."""
    ov = float(k.get("reset_at_override") or 0)
    if not ov:
        return
    interval_days = int(k.get("reset_interval_days") or 30)
    interval_s = interval_days * 86400
    now = time.time()
    while ov < now:
        ov += interval_s
    k["reset_at_override"] = ov
    # Mirror into reset_at if API never provides one
    if not k.get("reset_at"):
        k["reset_at"] = ov

def _key_is_live(k: dict) -> bool:
    _apply_reset_rollover(k)
    reset = float(k.get("reset_at") or 0)
    if reset and time.time() >= reset:
        k["credits_used"] = 0
        k["reset_at"]     = 0
        # Clear notify trackers for this key
        with _notified_lock:
            _notified_90pct.discard(k.get("label", ""))
            _notified_exhausted.discard(k.get("label", ""))
    limit = int(k.get("credits_limit") or DEFAULT_CREDIT_LIMITS.get(k.get("provider", ""), 0))
    used  = int(k.get("credits_used") or 0)
    return used < limit

def _pick_premium_key(provider: str, exclude_labels: set = None) -> Optional[dict]:
    with _keys_lock:
        live = [
            k for k in _keys
            if k.get("provider") == provider
            and k.get("role", "worker") == "worker"
            and _key_is_live(k)
            and (exclude_labels is None or k.get("label") not in exclude_labels)
        ]
    return random.choice(live) if live else None

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
        limit = int(key_dict.get("credits_limit") or 0)
        used  = int(key_dict["credits_used"])
        label = key_dict.get("label", "")

    if limit > 0:
        pct = used / limit
        if pct >= NOTIFY_90_PCT:
            _notify_once(_notified_90pct, label,
                         "⚠️ Key 90%",
                         f"{key_dict.get('provider')} / {label}: {used:,}/{limit:,} ({pct*100:.0f}%)",
                         "high", ["warning"])
        elif pct >= NOTIFY_75_PCT:
            _notify_once(_notified_90pct, f"{label}:75",
                         "ℹ️ Key 75%",
                         f"{key_dict.get('provider')} / {label}: {used:,}/{limit:,}",
                         "default", ["information_source"])

def _exhaust_key(key_dict: dict, reason: str = "credits hit limit"):
    limit = int(key_dict.get("credits_limit") or
                DEFAULT_CREDIT_LIMITS.get(key_dict.get("provider", ""), 0))
    with _keys_lock:
        key_dict["credits_used"] = limit
        key_dict["last_used"]    = time.time()
        label    = key_dict.get("label", "")
        provider = key_dict.get("provider", "")

    _notify_once(_notified_exhausted, label,
                 "🔴 Key exhausted",
                 f"{provider} / {label} — {reason}",
                 "urgent", ["rotating_light"])

    # Check if provider is now fully down
    _check_provider_exhausted(provider)

def _check_provider_exhausted(provider: str):
    with _keys_lock:
        prov_keys = [k for k in _keys
                     if k.get("provider") == provider
                     and k.get("role", "worker") == "worker"]
        live = [k for k in prov_keys if _key_is_live(k)]

    if prov_keys and not live:
        _notify_once(_notified_provider_down, provider,
                     "🚨 PROVIDER DOWN",
                     f"{provider}: 0 live keys of {len(prov_keys)}",
                     "urgent", ["skull"])

# ═══════════════════════════════════════════════════════════════════════════════
# WEIGHTED PROVIDER SELECTION
# ═══════════════════════════════════════════════════════════════════════════════
def _provider_weights() -> dict:
    """Weight each primary provider by sum of remaining credits across live worker keys."""
    now = time.time()
    weights = {}
    with _keys_lock:
        snapshot = list(_keys)
    for provider in PROVIDER_SPECS:
        spec = PROVIDER_SPECS[provider]
        if spec["tier"] != "primary":
            continue
        with _provider_cooldown_lock:
            if _provider_cooldown.get(provider, 0) > now:
                weights[provider] = 0.0
                continue
        total = 0
        for k in snapshot:
            if k.get("provider") != provider: continue
            if k.get("role", "worker") != "worker": continue
            if not _key_is_live(k): continue
            limit = int(k.get("credits_limit") or 0)
            used  = int(k.get("credits_used") or 0)
            total += max(0, limit - used)
        weights[provider] = float(total)
    return weights

def _plan_providers(plan: str) -> list:
    if plan == "sticky":
        return [p for p, s in PROVIDER_SPECS.items() if s["supports_sticky"]]

    weights   = _provider_weights()
    primaries = [p for p, s in PROVIDER_SPECS.items() if s["tier"] == "primary"]
    last_res = [p for p, s in PROVIDER_SPECS.items() if s["tier"] == "last_resort"]

    # Weighted random ordering of primaries
    remaining = list(primaries)
    ordered = []
    while remaining:
        w = [max(0.001, weights.get(p, 0.0)) for p in remaining]
        total = sum(w)
        if total <= 0:
            random.shuffle(remaining)
            ordered.extend(remaining)
            break
        r = random.random() * total
        cum = 0.0
        for i, p in enumerate(remaining):
            cum += w[i]
            if cum >= r:
                ordered.append(p)
                remaining.pop(i)
                break
        else:
            ordered.extend(remaining)
            break
    return ordered + last_res

def _mark_provider_ok(provider: str):
    with _provider_cooldown_lock:
        _provider_fail_streak[provider] = 0
        _provider_cooldown.pop(provider, None)

def _mark_provider_fail(provider: str):
    with _provider_cooldown_lock:
        _provider_fail_streak[provider] = _provider_fail_streak.get(provider, 0) + 1
        if _provider_fail_streak[provider] >= PROVIDER_FAIL_THRESHOLD:
            _provider_cooldown[provider] = time.time() + PROVIDER_COOLDOWN_S

# ═══════════════════════════════════════════════════════════════════════════════
# SYNC
# ═══════════════════════════════════════════════════════════════════════════════
def _sync_one_key(key_dict: dict) -> dict:
    provider = key_dict.get("provider", "")
    fn = _USAGE_FETCHERS.get(provider)
    if not fn:
        return {"label": key_dict.get("label"), "provider": provider,
                "ok": False, "error": "no usage fetcher"}
    try:
        limit, used, reset = fn(key_dict.get("key", ""))
    except Exception as e:
        return {"label": key_dict.get("label"), "provider": provider,
                "ok": False, "error": str(e)}
    with _keys_lock:
        key_dict["credits_limit"] = limit
        key_dict["credits_used"]  = used
        key_dict["last_sync"]     = time.time()
        # Only overwrite reset_at if API gave us one
        if reset:
            key_dict["reset_at"] = reset
        key_dict["credit_source"] = "api"
        # Clear 90% notify if usage dropped (reset happened)
        if used < limit * 0.5:
            label = key_dict.get("label", "")
            with _notified_lock:
                _notified_90pct.discard(label)
                _notified_90pct.discard(f"{label}:75")
                _notified_exhausted.discard(label)
    return {"label": key_dict.get("label"), "provider": provider, "ok": True,
            "credits_limit": limit, "credits_used": used, "reset_at": reset}


def _sync_all_keys() -> dict:
    """Sync uses ONLY usage_check keys per provider. Falls back to worker if none."""
    with _keys_lock:
        snapshot = list(_keys)

    # Group by provider, prefer role=usage_check
    by_prov: dict = defaultdict(lambda: {"usage_check": None, "workers": []})
    for k in snapshot:
        prov = k.get("provider", "")
        if k.get("role") == "usage_check":
            by_prov[prov]["usage_check"] = k
        else:
            by_prov[prov]["workers"].append(k)

    results = []
    for prov, group in by_prov.items():
        sync_key = group["usage_check"]
        if sync_key is None:
            # No dedicated key — sync via first live worker
            for w in group["workers"]:
                if _key_is_live(w):
                    sync_key = w
                    break
        if sync_key is None:
            results.append({"label": f"{prov}::nosync", "provider": prov,
                            "ok": False, "error": "no sync-capable key"})
            continue

        # Fire the provider API call
        r = _sync_one_key(sync_key)
        r["used_for_sync"] = sync_key.get("label")
        results.append(r)

        # Mirror the account-level credits onto workers that lack API sync
        if r.get("ok"):
            with _keys_lock:
                for w in group["workers"]:
                    if w.get("label") == sync_key.get("label"):
                        continue
                    # Only mirror if worker has never been api-synced
                    if w.get("credit_source") != "api":
                        # Approximate: keep worker's own used count vs account limit
                        if w.get("credits_limit", 0) < r.get("credits_limit", 0):
                            w["credits_limit"] = r.get("credits_limit")

    _save_keys()
    ok_count   = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    return {"ok": ok_count, "failed": fail_count, "total": len(results),
            "results": results, "results_count": len(results)}


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
    entry = {"ts": time.time(), "role": ROLE, "event": event,
             "detail": detail, "count": count}
    with _activity_lock:
        _activity_log.append(entry)
        if len(_activity_log) > ACTIVITY_MAX:
            del _activity_log[:-ACTIVITY_MAX]

def _assign_cat(avg_s: float):
    if avg_s < CAT_FLASH:   return "flash"
    if avg_s < CAT_PANTHER: return "panther"
    if avg_s < CAT_LANTERN: return "lantern"
    if avg_s < CAT_DEAD:    return "deadass"
    return None

def _meesho_headers():
    return {
        "authorization": MEESHO_AUTH, "app-version": "29.3",
        "app-version-code": "864", "app-client-id": "android",
        "app-sdk-version": "36", "application-id": APP_ID,
        "country-iso": "in", "instance-id": uuid.uuid4().hex,
        "app-session-id": str(uuid.uuid4()), "app-session-count": "1",
        "app-gaid": str(uuid.uuid4()), "shield-session-id": str(uuid.uuid4()),
        "meesho-user-context": "anonymous",
        "content-type": "application/json; charset=UTF-8",
        "user-agent": "okhttp/4.9.0", "accept-encoding": "gzip",
    }

def _decode_meesho(raw, enc):
    try:
        if "gzip"    in enc: raw = gzip.decompress(raw)
        elif "deflate" in enc: raw = zlib.decompress(raw)
    except Exception: pass
    try: return json.loads(raw)
    except Exception: return {}

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

def _gh_headers():
    h = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN: h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE FETCHERS (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
def _fetch_github_raw(owner, repo, path, sha="main"):
    try:
        r = requests.get(f"https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{path}",
                         headers=_gh_headers(), timeout=10)
        if r.status_code == 200: return _parse(r.text)
    except Exception: pass
    return []

def _fetch_github_latest(owner, repo, path):
    try:
        r = requests.get(f"https://api.github.com/repos/{owner}/{repo}/commits",
                         params={"path": path, "per_page": 1},
                         headers=_gh_headers(), timeout=8)
        if r.status_code in (403, 429):
            _inc("gh_ratelimited"); return [], ""
        if r.status_code != 200: return [], ""
        commits = r.json()
        if not isinstance(commits, list) or not commits: return [], ""
        sha = commits[0].get("sha", "")
        if not sha: return [], ""
    except Exception: return [], ""
    return _fetch_github_raw(owner, repo, path, sha), sha

def _fetch_github_commit(owner, repo, path):
    key = f"{owner}/{repo}/{path}"
    proxies, sha = _fetch_github_latest(owner, repo, path)
    if not sha: return [], ""
    with _sha_lock:
        if sha == _repo_sha.get(key, ""): return [], ""
        _repo_sha[key] = sha
    return proxies[:COMMIT_FETCH_COUNT], sha

def _fetch_http_url(url):
    hdrs = {}
    with _etag_lock:
        if url in _http_etag: hdrs["If-None-Match"]     = _http_etag[url]
        if url in _http_lmod: hdrs["If-Modified-Since"] = _http_lmod[url]
    try:
        r = requests.get(url, headers=hdrs, timeout=10)
        if r.status_code == 304: return []
        with _etag_lock:
            if "ETag"          in r.headers: _http_etag[url] = r.headers["ETag"]
            if "Last-Modified" in r.headers: _http_lmod[url] = r.headers["Last-Modified"]
        return _parse(r.text)
    except Exception: return []

def _fetch_geonode_page(url):
    try:
        r = requests.get(url, timeout=10)
        return [f"{p['ip']}:{p['port']}" for p in r.json().get("data", [])]
    except Exception: return []

def _fetch_html_page(url):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        out = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    ip, port = cols[0].get_text(strip=True), cols[1].get_text(strip=True)
                    if re.match(r"\d{1,3}(?:\.\d{1,3}){3}", ip) and port.isdigit():
                        out.append(f"{ip}:{port}")
        return out
    except Exception: return []

def _fetch_checkerproxy():
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r = requests.get(f"https://checkerproxy.net/api/archive/{today}", timeout=10)
        return [p["addr"] for p in r.json()
                if p.get("addr") and re.match(r"\d+\.\d+\.\d+\.\d+:\d+", p["addr"])]
    except Exception: return []

def _bulk_fetch(repos, http_srcs, geonode, html, limit):
    collected = set()
    results = []
    def _add(proxies):
        for p in proxies:
            if p not in collected:
                collected.add(p); results.append(p)
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {}
        for o, r, p in repos: futs[ex.submit(_fetch_github_raw, o, r, p)] = f"gh"
        for url in http_srcs: futs[ex.submit(_fetch_http_url, url)] = f"http"
        for url in geonode:   futs[ex.submit(_fetch_geonode_page, url)] = f"geo"
        for url in html:      futs[ex.submit(_fetch_html_page, url)] = f"html"
        futs[ex.submit(_fetch_checkerproxy)] = "checkerproxy"
        for fut in as_completed(futs):
            try: _add(fut.result() or [])
            except Exception: pass
            if len(results) >= limit: break
    return results[:limit]

# ═══════════════════════════════════════════════════════════════════════════════
# MEESHO CHECK
# ═══════════════════════════════════════════════════════════════════════════════
async def _meesho_check(session, addr):
    hdr = _meesho_headers()
    t0  = asyncio.get_event_loop().time()
    try:
        async with session.get(f"{MEESHO_API}/api/1.0/anonymous/config",
                               headers=hdr, proxy=f"http://{addr}",
                               timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT_S),
                               ssl=False) as r:
            if r.status == 200:
                raw = await r.read()
                data = _decode_meesho(raw, r.headers.get("Content-Encoding", ""))
                xoox = data.get("xoox", {})
                if isinstance(xoox, str):
                    try: xoox = json.loads(xoox)
                    except Exception: xoox = {}
                elapsed = asyncio.get_event_loop().time() - t0
                if isinstance(xoox, dict) and xoox.get("xo"):
                    return True, elapsed
    except Exception: pass
    return False, 0.0

# ═══════════════════════════════════════════════════════════════════════════════
# POOL ROLE
# ═══════════════════════════════════════════════════════════════════════════════
def _push_to_pool(addr, latency_s):
    with _outbuf_lock:
        _outbuf.append((addr, latency_s))

def _outbuf_sender():
    pool_url = PEERS.get("pool", "")
    while True:
        with _outbuf_lock: pending = len(_outbuf)
        time.sleep(0.1 if pending > 50 else INGEST_INTERVAL_S)
        with _outbuf_lock:
            if not _outbuf: continue
            batch = list(_outbuf[:INGEST_BATCH_SIZE])
            del _outbuf[:INGEST_BATCH_SIZE]
        if not pool_url: continue
        try:
            payload = [{"addr": a, "latency_s": l} for a, l in batch]
            requests.post(f"{pool_url}/ingest", json={"proxies": payload},
                          headers={"X-Secret": SHARED_SECRET}, timeout=10)
        except Exception:
            with _outbuf_lock:
                for item in batch: _outbuf.insert(0, item)

async def _checker_pipeline():
    global _check_queue
    _check_queue = asyncio.Queue(maxsize=10_000)
    sem = asyncio.Semaphore(CHECK_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CHECK_CONCURRENCY + 50,
                                     ttl_dns_cache=300, enable_cleanup_closed=True)
    async def _worker(session):
        while True:
            addr = await _check_queue.get()
            async with sem:
                passed, latency = await _meesho_check(session, addr)
            if passed:
                cat = _assign_cat(latency)
                if cat:
                    _inc(f"check_pass_{cat}")
                    _push_to_pool(addr, latency)
                else: _inc("check_too_slow")
            else: _inc("check_fail")
    async def _stats_printer():
        while True:
            await asyncio.sleep(15)
            q = _check_queue.qsize() if _check_queue else 0
            with _outbuf_lock: ob = len(_outbuf)
            with _counter_lock:
                cycle = dict(_counters_cycle)
            cats = ('flash', 'panther', 'lantern', 'deadass')
            print(f"[{ROLE}] q={q} outbuf={ob} | "
                  f"pass={sum(cycle.get(f'check_pass_{c}', 0) for c in cats)} "
                  f"fail={cycle.get('check_fail', 0)}", flush=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [asyncio.create_task(_worker(session)) for _ in range(CHECK_CONCURRENCY)]
        stats   = asyncio.create_task(_stats_printer())
        await asyncio.gather(*workers, stats)

def _checker_pipeline_thread():
    global _check_loop
    _check_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_check_loop)
    _check_loop.run_until_complete(_checker_pipeline())

def _enqueue(addr):
    if _seen_dup(addr): return
    if _check_queue and _check_loop and not _check_loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(_check_queue.put(addr), _check_loop)
        except RuntimeError: pass

def _checker_main():
    repos, http_srcs, geonode, html = _get_my_sources()
    boot = _bulk_fetch(repos, http_srcs, geonode, html, BOOT_FETCH_COUNT)
    for addr in boot: _enqueue(addr)
    def _init_sha():
        for o, r, p in repos:
            _, sha = _fetch_github_latest(o, r, p)
            if sha:
                with _sha_lock: _repo_sha[f"{o}/{r}/{p}"] = sha
    threading.Thread(target=_init_sha, daemon=True).start()
    last_poll = time.time()
    while True:
        time.sleep(5)
        now = time.time()
        _reset_cycle()
        if now - last_poll >= COMMIT_POLL_S:
            last_poll = now
            commit_found = False
            for o, r, p in repos:
                new, _ = _fetch_github_commit(o, r, p)
                if new:
                    commit_found = True
                    for addr in new: _enqueue(addr)
            if commit_found: continue
        q = _check_queue.qsize() if _check_queue else 0
        if q > 2000: continue
        batch = _bulk_fetch(repos, http_srcs, geonode, html, CYCLE_FETCH_COUNT)
        for addr in batch: _enqueue(addr)

def _start_checker():
    threading.Thread(target=_checker_pipeline_thread, daemon=True).start()
    deadline = time.time() + 10
    while _check_queue is None and time.time() < deadline: time.sleep(0.05)
    threading.Thread(target=_outbuf_sender, daemon=True).start()
    threading.Thread(target=_checker_main, daemon=True).start()
    print(f"[{ROLE}] checker started", flush=True)

def _promote(addr, latency_s):
    cat = _assign_cat(latency_s)
    if cat is None:
        _inc("pool_rejected_slow"); return
    now = time.time()
    with _proxies_lock:
        rec = _proxies.get(addr)
        if rec is None:
            rec = {"received_at": now}; _proxies[addr] = rec
        rec["last_checked"] = now
        rec["latency_ms"]   = int(latency_s * 1000)
        rec["label"]        = cat
        rec["next_check"]   = now + POOL_RECHECK_MIN_AGE_S
    _inc("pool_promoted")
    _persist_event.set()

def _evict(addr, reason="dead"):
    with _proxies_lock:
        existed = _proxies.pop(addr, None) is not None
    if existed:
        _inc("pool_evicted")
        _persist_event.set()
    with _used_lock: _last_used.pop(addr, None)

def _pick(purpose="backend", caller="unknown"):
    now = time.time()
    with _proxies_lock:
        items = list(_proxies.items())
    if not items: return {}
    live_sorted = sorted(items, key=lambda x: x[1].get("last_checked", 0), reverse=True)
    selected = None
    with _used_lock:
        for addr, rec in live_sorted:
            if now - _last_used.get(addr, 0) >= POOL_COOLDOWN_S:
                selected = (addr, rec); break
    if selected is None: selected = live_sorted[0]
    addr, rec = selected
    with _used_lock: _last_used[addr] = now
    with _proxies_lock:
        cur = _proxies.get(addr)
        if cur: cur["next_check"] = now + POOL_RECHECK_MIN_AGE_S
    _inc("pool_picks")
    _inc(f"pool_picks_by_{caller}")
    return {"http": f"http://{addr}", "https": f"http://{addr}", "addr": addr}

def _pick_race_candidates(caller="unknown", count=RACE_PROXY_COUNT):
    now = time.time()
    with _proxies_lock:
        items = list(_proxies.items())
    if not items: return []
    fresh = []
    for addr, rec in items:
        ca = rec.get("last_checked", 0)
        ls = rec.get("latency_ms", 0) / 1000.0
        if now - 30 <= ca <= now and 0 < ls < RACE_FRESH_MAX_S:
            fresh.append((ca, addr, rec))
    fresh.sort(key=lambda x: x[0], reverse=True)
    if fresh:
        selected = [(a, r) for _, a, r in fresh[:count]]
    else:
        selected = sorted(items, key=lambda x: x[1].get("last_checked", 0),
                          reverse=True)[:count]
    if not selected: return []
    with _used_lock:
        for a, _ in selected: _last_used[a] = now
    with _proxies_lock:
        for a, _ in selected:
            cur = _proxies.get(a)
            if cur: cur["next_check"] = now + POOL_RECHECK_MIN_AGE_S
    _inc("pool_race_sets")
    return [{"http": f"http://{a}", "https": f"http://{a}", "addr": a}
            for a, _ in selected]

def mark_dead(addr):
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
            lc = r.get("last_checked", 0)
            age = (now - lc) if lc else 1e9
            if lc and now - r.get("received_at", now) > POOL_TTL_S:
                stale.append(a); continue
            if age <= FRESH_COLD_S:
                f120.append(a)
                if r.get("label") in ("flash", "panther"): fast.append(a)
                if age <= FRESH_HOT_S: f30.append(a)
        for a in stale: _evict(a, "ttl_expired")
        with _snap_lock:
            _snap_fresh30, _snap_fresh120, _snap_fast = f30, f120, fast
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
            if not batch: continue
            async def _check_one(addr):
                async with sem:
                    passed, latency = await _meesho_check(session, addr)
                if passed:
                    _promote(addr, latency); _inc("recheck_pass")
                else:
                    _evict(addr, "recheck_fail"); _inc("recheck_fail")
            await asyncio.gather(*[_check_one(a) for a in batch])

def _pool_recheck_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_pool_recheck_worker())

def _persist_worker():
    while True:
        _persist_event.wait(timeout=PERSIST_INTERVAL_S)
        _persist_event.clear()
        if ROLE == "pool": _save_free()
        elif ROLE == "vps": _save_keys()

def _start_pool():
    _load_free()
    threading.Thread(target=_snapshot_worker, daemon=True).start()
    threading.Thread(target=_pool_recheck_thread, daemon=True).start()
    threading.Thread(target=_persist_worker, daemon=True).start()

# ═══════════════════════════════════════════════════════════════════════════════
# VPS ROLE
# ═══════════════════════════════════════════════════════════════════════════════
def _get_proxy_from_pool(cat="any", purpose="backend"):
    url = PEERS.get("pool", "")
    if not url: return {}
    try:
        r = requests.get(f"{url}/pick", params={"cat": cat, "purpose": purpose},
                         headers={"X-Secret": SHARED_SECRET}, timeout=5)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return {}

def _get_race_proxies_from_pool(count=RACE_PROXY_COUNT):
    url = PEERS.get("pool", "")
    if not url: return []
    try:
        r = requests.get(f"{url}/pick-many", params={"count": count},
                         headers={"X-Secret": SHARED_SECRET}, timeout=6)
        if r.status_code == 200:
            return r.json().get("proxies", [])
    except Exception: pass
    return []

def _report_dead_to_pool(addr):
    url = PEERS.get("pool", "")
    if not url: return
    try:
        requests.post(f"{url}/dead", json={"addr": addr},
                      headers={"X-Secret": SHARED_SECRET}, timeout=3)
    except Exception: pass

def _pinger():
    order = ["checker_1","checker_2","checker_3","checker_4","checker_5",
             "checker_6","checker_7","checker_8","checker_9","checker_10",
             "pool","dashboard"]
    while True:
        for role in order:
            url = PEERS.get(role, "")
            if not url: continue
            try: requests.get(f"{url}/health", timeout=5)
            except Exception: pass
            time.sleep(3)
        time.sleep(PING_INTERVAL)

def _start_vps():
    _load_keys()
    threading.Thread(target=_pinger, daemon=True).start()
    threading.Thread(target=_persist_worker, daemon=True).start()
    live = sum(1 for k in _keys if _key_is_live(k))
    notify("ProxyStack boot",
           f"VPS booted — {len(_keys)} keys, {live} live",
           "low", ["rocket"])
    print(f"[vps] started — {len(_keys)} keys ({live} live)", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PERSIST
# ═══════════════════════════════════════════════════════════════════════════════
def _save_free():
    try:
        with _proxies_lock:
            data = {a: {"label": r.get("label"),
                        "latency_ms": r.get("latency_ms"),
                        "last_checked": r.get("last_checked", 0),
                        "received_at": r.get("received_at", 0)}
                    for a, r in _proxies.items()}
        PERSIST_FREE.write_text(json.dumps(data))
    except Exception as e:
        print(f"[persist] free save: {e}", flush=True)

def _load_free():
    if not PERSIST_FREE.exists(): return
    try:
        data = json.loads(PERSIST_FREE.read_text())
        now  = time.time()
        rows = data.items() if isinstance(data, dict) else [(a, {}) for a in data]
        with _proxies_lock:
            for addr, r in rows:
                if now - r.get("received_at", 0) > POOL_TTL_S: continue
                _proxies[addr] = {"received_at": r.get("received_at", now),
                                  "last_checked": r.get("last_checked", 0),
                                  "latency_ms": r.get("latency_ms", 0),
                                  "label": r.get("label"), "next_check": now}
    except Exception as e:
        print(f"[persist] free load: {e}", flush=True)

def _save_keys():
    try:
        with _keys_lock:
            keys = [dict(k) for k in _keys]
        KEYS_FILE.write_text(json.dumps(keys, indent=2))
    except Exception as e:
        print(f"[persist] keys save: {e}", flush=True)

def _load_keys():
    if not KEYS_FILE.exists(): return
    try:
        data = json.loads(KEYS_FILE.read_text())
        with _keys_lock:
            for entry in data:
                entry.setdefault("credits_used", 0)
                entry.setdefault("credits_limit",
                                 DEFAULT_CREDIT_LIMITS.get(entry.get("provider", ""), 0))
                entry.setdefault("reset_at", 0)
                entry.setdefault("reset_at_override", 0)
                entry.setdefault("reset_interval_days", 30)
                entry.setdefault("last_used", 0)
                entry.setdefault("credit_source", "local")
                entry.setdefault("last_sync", 0)
                entry.setdefault("role", "worker")
                prov = entry.get("provider", "")
                entry.setdefault("renewal",
                                 PROVIDER_SPECS.get(prov, {}).get("renewal", "renewable"))
                _keys.append(entry)
        print(f"[persist] loaded {len(data)} keys", flush=True)
    except Exception as e:
        print(f"[persist] keys load: {e}", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════════
def _get_stats():
    now = time.time()
    with _proxies_lock:
        items = list(_proxies.items())
    labs = defaultdict(int)
    for _, r in items:
        if r.get("label"): labs[r["label"]] += 1

    with _keys_lock:
        keys_snap = [dict(k) for k in _keys]

    providers_block = {}
    for k in keys_snap:
        prov = k.get("provider", "")
        limit = int(k.get("credits_limit") or DEFAULT_CREDIT_LIMITS.get(prov, 0))
        used  = int(k.get("credits_used") or 0)
        role  = k.get("role", "worker")
        if role == "usage_check": continue
        b = providers_block.setdefault(prov, {
            "keys": 0, "live": 0, "total_credits": 0, "used_credits": 0,
            "renewal": PROVIDER_SPECS.get(prov, {}).get("renewal", "unknown"),
            "weight": 0.0,
        })
        b["keys"] += 1; b["total_credits"] += limit; b["used_credits"] += used
        if used < limit: b["live"] += 1

    weights = _provider_weights()
    total_w = sum(weights.values()) or 1
    for prov, w in weights.items():
        if prov in providers_block:
            providers_block[prov]["weight"] = round(w / total_w * 100, 1)

    with _counter_lock:
        life, cyc = dict(_counters_lifetime), dict(_counters_cycle)
    with _activity_lock:
        acts = list(reversed(_activity_log))[:50]
    q = _check_queue.qsize() if _check_queue else 0

    with _provider_cooldown_lock:
        cooldowns = {p: round(u - now, 1) for p, u in _provider_cooldown.items() if u > now}
        fail_streaks = dict(_provider_fail_streak)

    return {
        "role": ROLE,
        "counters": {"lifetime": life, "cycle": cyc},
        "activity": acts,
        "queues": {"check": q, "outbuf": len(_outbuf)},
        "free": {"live": len(items), "flash": labs["flash"],
                 "panther": labs["panther"], "lantern": labs["lantern"],
                 "deadass": labs["deadass"]},
        "providers": providers_block,
        "provider_cd": cooldowns,
        "provider_fail": fail_streaks,
        "peers": PEERS,
        "notify_enabled": NTFY_ENABLED,
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
    if ROLE.startswith("checker_"): _start_checker()
    elif ROLE == "pool":            _start_pool()
    elif ROLE == "vps":             _start_vps()
    print(f"[ProxyStack v2] {ROLE} booted on port {PORT}", flush=True)

@app.get("/health")
def health(): return {"ok": True, "role": ROLE, "notify": NTFY_ENABLED}

@app.get("/stats")
async def stats():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_api_executor, _get_stats)

async def _sse_generator():
    loop = asyncio.get_event_loop()
    while True:
        try:
            if ROLE == "dashboard":
                merged = {"role": ROLE, "nodes": {}, "ts": time.time()}
                for role, url in PEERS.items():
                    try:
                        r = await asyncio.wait_for(loop.run_in_executor(
                            _api_executor,
                            lambda u=url: requests.get(f"{u}/stats", timeout=2).json()),
                            timeout=3.0)
                        merged["nodes"][role] = r
                    except Exception:
                        merged["nodes"][role] = {"error": "unreachable"}
                yield f"data: {json.dumps(merged)}\n\n"
            else:
                data = await asyncio.wait_for(loop.run_in_executor(_api_executor, _get_stats), 2.0)
                data["ts"] = time.time()
                yield f"data: {json.dumps(data)}\n\n"
        except asyncio.TimeoutError:
            yield ": keepalive\n\n"
        await asyncio.sleep(2)

@app.get("/stream/stats")
async def stream_stats():
    return StreamingResponse(_sse_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

class IngestItem(BaseModel):
    addr: str; latency_s: float = 5.0
class IngestRequest(BaseModel):
    proxies: list[IngestItem]

@app.post("/ingest")
async def ingest(req: IngestRequest, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET: raise HTTPException(403, "invalid secret")
    if ROLE != "pool": raise HTTPException(400, "ingest only on pool role")
    for item in req.proxies: _promote(item.addr, item.latency_s)
    return {"ingested": len(req.proxies)}

@app.get("/pick")
async def pick_route(request: Request, cat: str = "any",
                     purpose: str = "backend",
                     x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET: raise HTTPException(403, "invalid secret")
    if ROLE != "pool": raise HTTPException(400, "pick only on pool role")
    caller = _identify_caller(request)
    loop = asyncio.get_event_loop()
    p = await loop.run_in_executor(_api_executor, _pick, purpose, caller)
    if not p: raise HTTPException(503, "no live proxies")
    return p

@app.get("/pick-many")
async def pick_many_route(request: Request, count: int = RACE_PROXY_COUNT,
                          x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET: raise HTTPException(403, "invalid secret")
    if ROLE != "pool": raise HTTPException(400, "pick-many only on pool role")
    count = max(1, min(count, RACE_PROXY_COUNT))
    caller = _identify_caller(request)
    loop = asyncio.get_event_loop()
    p = await loop.run_in_executor(_api_executor, _pick_race_candidates, caller, count)
    if not p: raise HTTPException(503, "no live proxies")
    return {"proxies": p}

class DeadReport(BaseModel):
    addr: str

@app.post("/dead")
async def report_dead(req: DeadReport, x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET: raise HTTPException(403, "invalid secret")
    if ROLE != "pool": raise HTTPException(400, "dead report only on pool role")
    mark_dead(req.addr)
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════════════════════
# VPS /request
# ═══════════════════════════════════════════════════════════════════════════════
class ProxyRequest(BaseModel):
    url: str
    method: str = "GET"
    headers: Optional[dict] = None
    body: Optional[dict] = None
    params: Optional[dict] = None
    retries: int = 5
    timeout: int = 30
    tier: str = "any"
    category: str = "any"

async def _do_free_race(req, loop):
    method = req.method.upper()
    proxies = await loop.run_in_executor(_backend_executor,
                                         _get_race_proxies_from_pool, RACE_PROXY_COUNT)
    if not proxies: return None
    connector = aiohttp.TCPConnector(limit=len(proxies) + 5, ssl=False,
                                     enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def _one(prx):
            addr = prx.get("addr", ""); purl = prx.get("http", "")
            if not purl: return None
            try:
                async with session.request(method=method, url=req.url,
                                           headers=req.headers or {},
                                           json=req.body if method in ("POST","PUT","PATCH") else None,
                                           params=req.params, proxy=purl,
                                           timeout=aiohttp.ClientTimeout(total=RACE_WAIT_S)) as r:
                    raw = await r.read()
                    enc = r.headers.get("Content-Encoding", "")
                    ct  = r.headers.get("Content-Type", "application/octet-stream")
                    try:
                        if "gzip" in enc:    raw = gzip.decompress(raw)
                        elif "deflate" in enc: raw = zlib.decompress(raw)
                    except Exception: pass
                    if r.status < 400:
                        return {"addr": addr, "status": r.status, "raw": raw, "ct": ct}
            except Exception:
                if addr:
                    loop.run_in_executor(_backend_executor, _report_dead_to_pool, addr)
            return None
        tasks = [asyncio.create_task(_one(p)) for p in proxies]
        try:
            for c in asyncio.as_completed(tasks, timeout=RACE_WAIT_S + 0.2):
                res = await c
                if res is None: continue
                for t in tasks:
                    if not t.done(): t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                _inc("race_wins")
                return Response(content=res["raw"], status_code=res["status"], media_type=res["ct"])
        except asyncio.TimeoutError: pass
        finally:
            for t in tasks:
                if not t.done(): t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    _inc("race_all_failed")
    return None

async def _do_free_single(req, loop):
    prx = await loop.run_in_executor(_backend_executor, _get_proxy_from_pool, "any", "fod")
    if not prx: raise HTTPException(503, "no free proxies")
    purl = prx.get("http", ""); addr = prx.get("addr", "")
    method = req.method.upper()
    connector = aiohttp.TCPConnector(ssl=False, enable_cleanup_closed=True)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.request(method=method, url=req.url,
                                       headers=req.headers or {},
                                       json=req.body if method in ("POST","PUT","PATCH") else None,
                                       params=req.params, proxy=purl,
                                       timeout=aiohttp.ClientTimeout(total=req.timeout or 30)) as r:
                raw = await r.read()
                enc = r.headers.get("Content-Encoding", "")
                ct  = r.headers.get("Content-Type", "application/octet-stream")
                try:
                    if "gzip" in enc:    raw = gzip.decompress(raw)
                    elif "deflate" in enc: raw = zlib.decompress(raw)
                except Exception: pass
                return Response(content=raw, status_code=r.status, media_type=ct)
    except asyncio.TimeoutError:
        if addr: await loop.run_in_executor(_backend_executor, _report_dead_to_pool, addr)
        raise HTTPException(504, "free single timeout")
    except Exception as e:
        if addr: await loop.run_in_executor(_backend_executor, _report_dead_to_pool, addr)
        raise HTTPException(502, f"free single failed: {e}")

def _provider_call_sync(provider, method, url, headers, body, session,
                        key_label=None, timeout=None):
    fn = _PROVIDER_CALLS.get(provider)
    if not fn: return {"success": False, "error": f"unknown {provider}"}
    return fn(method, url, headers, body, session=session,
              key_label=key_label, timeout=timeout)

def _derive_sticky_id(req) -> int:
    low = {str(k).lower(): v for k, v in (req.headers or {}).items()}
    sid = low.get("app-session-id") or low.get("instance-id") or ""
    if not sid:
        sid = req.url + json.dumps(req.body or {}, sort_keys=True)
    return int(hashlib.md5(sid.encode()).hexdigest()[:8], 16) % 1_000_000

def _sticky_pin(flow_key):
    now = time.time()
    with _sticky_lock:
        entry = _sticky_sessions.get(str(flow_key))
        if entry and entry.get("expires_at", 0) > now:
            entry["expires_at"] = now + STICKY_TTL_S
            return entry
    for provider in (p for p, s in PROVIDER_SPECS.items() if s["supports_sticky"]):
        key = _pick_premium_key(provider)
        if key:
            entry = {"provider": provider, "key_label": key.get("label"),
                     "session_number": flow_key,
                     "expires_at": now + STICKY_TTL_S}
            with _sticky_lock: _sticky_sessions[str(flow_key)] = entry
            return entry
    return None

async def _premium_forward(req, plan, loop):
    method = req.method.upper()
    timeout = _timeout_for_url(req.url)
    providers = _plan_providers(plan)
    sticky_entry = None
    if plan == "sticky":
        flow_key = _derive_sticky_id(req)
        sticky_entry = await loop.run_in_executor(_backend_executor, _sticky_pin, flow_key)
        if not sticky_entry:
            raise HTTPException(503, "no sticky provider")
        providers = [sticky_entry["provider"]]

    last_err = None
    tried = []
    for provider in providers:
        pinned = sticky_entry.get("key_label") if sticky_entry else None
        sess   = sticky_entry.get("session_number") if sticky_entry else None
        tried.append(provider)
        result = await loop.run_in_executor(
            _backend_executor, _provider_call_sync,
            provider, method, req.url, req.headers or {},
            req.body if method in ("POST","PUT","PATCH") else None,
            sess, pinned, timeout,
        )
        if result.get("success"):
            _mark_provider_ok(provider)
            data = result.get("data")
            if isinstance(data, (dict, list)):
                raw, ct = json.dumps(data).encode(), "application/json"
            else:
                raw, ct = str(data).encode(), "text/plain"
            _inc(f"premium_ok_{provider}")
            return Response(content=raw, status_code=200, media_type=ct)
        _mark_provider_fail(provider)
        last_err = result.get("error", "unknown")
        _inc(f"premium_fail_{provider}")

    # All primaries exhausted → check if we fell to last-resort
    notify("🚨 All providers failed",
           f"url={req.url[:80]} — tried: {','.join(tried)} — last: {last_err}",
           "urgent", ["skull", "warning"])
    raise HTTPException(502, f"all providers failed: {last_err}")

@app.post("/request")
async def proxy_request(req: ProxyRequest):
    if ROLE != "vps": raise HTTPException(400, "/request only on vps role")
    loop = asyncio.get_event_loop()
    if req.tier == "premium" or req.tier == "paid":
        _inc("requests_premium_forced")
        return await _premium_forward(req, "read", loop)
    if req.tier == "free_only":
        _inc("requests_free_only")
        return await _do_free_single(req, loop)
    if _is_fod_url(req.url) and req.tier == "any":
        _inc("requests_fod_default_free")
        return await _do_free_single(req, loop)
    plan = _plan_for_url(req.url)
    if plan == "read":
        _inc("requests_read")
        result = await _do_free_race(req, loop)
        if result is not None: return result
        return await _premium_forward(req, "read", loop)
    if plan == "write":
        _inc("requests_write")
        return await _premium_forward(req, "write", loop)
    if plan == "sticky":
        _inc("requests_sticky")
        return await _premium_forward(req, "sticky", loop)
    raise HTTPException(500, "unknown plan")

# ═══════════════════════════════════════════════════════════════════════════════
# KEY MANAGEMENT ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════
class AddKeyRequest(BaseModel):
    provider: str
    key: str
    label: Optional[str] = ""
    credits_limit: Optional[int] = None
    role: Optional[str] = "worker"
    renewal: Optional[str] = None
    reset_at_override: Optional[float] = None
    reset_interval_days: Optional[int] = 30

class RemoveKeyRequest(BaseModel):
    key: Optional[str] = None
    label: Optional[str] = None

class ImportKeysRequest(BaseModel):
    keys: list[dict]

def _vps_forward(method, path, body=None):
    base = PEERS.get("vps", "").rstrip("/")
    if not base: return 503, {"error": "vps not configured"}
    try:
        r = requests.request(method, f"{base}{path}", json=body,
                             headers={"X-Secret": SHARED_SECRET}, timeout=30)
        try: data = r.json() if r.content else {}
        except Exception: data = {"raw": r.text[:300]}
        return r.status_code, data
    except Exception as e:
        return 502, {"error": str(e)}

@app.post("/keys/add")
async def add_key(req: AddKeyRequest):
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/add", req.dict())
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")
    provider = req.provider.strip().lower()
    if provider not in PROVIDERS:
        raise HTTPException(400, f"unknown provider {provider}")
    auto_label = req.label or f"{provider}-{req.key[:8]}"
    with _keys_lock:
        if any(k["key"] == req.key for k in _keys):
            raise HTTPException(400, "key exists")
        final = auto_label
        i = 2
        while any(k["label"] == final for k in _keys):
            final = f"{auto_label}-{i}"; i += 1
        entry = {
            "provider": provider, "key": req.key, "label": final,
            "credits_limit": req.credits_limit or DEFAULT_CREDIT_LIMITS.get(provider, 1000),
            "credits_used": 0, "reset_at": 0,
            "reset_at_override": req.reset_at_override or 0,
            "reset_interval_days": req.reset_interval_days or 30,
            "last_used": 0, "credit_source": "local", "last_sync": 0,
            "role": req.role or "worker",
            "renewal": req.renewal or PROVIDER_SPECS[provider]["renewal"],
        }
        _keys.append(entry)
        with _key_sems_lock:
            _key_sems[final] = threading.Semaphore(KEY_CONCURRENCY_LIMITS.get(provider, 1))
    _save_keys()
    return {"status": "ok", "label": final, "provider": provider}

@app.post("/keys/import")
async def import_keys(req: ImportKeysRequest):
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/import", req.dict())
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")

    added = 0; skipped = 0
    with _keys_lock:
        existing_keys = {k["key"] for k in _keys}
        for entry in req.keys:
            prov = entry.get("provider", "").lower()
            if prov not in PROVIDERS:
                skipped += 1; continue
            if entry.get("key") in existing_keys:
                skipped += 1; continue
            label = entry.get("label") or f"{prov}-{entry['key'][:8]}"
            final = label; i = 2
            existing_labels = {k["label"] for k in _keys}
            while final in existing_labels:
                final = f"{label}-{i}"; i += 1
            full = {
                "provider": prov,
                "key": entry["key"],
                "label": final,
                "credits_limit": entry.get("credits_limit") or DEFAULT_CREDIT_LIMITS.get(prov, 1000),
                "credits_used": entry.get("credits_used", 0),
                "reset_at": entry.get("reset_at", 0),
                "reset_at_override": entry.get("reset_at_override", 0),
                "reset_interval_days": entry.get("reset_interval_days", 30),
                "last_used": 0,
                "credit_source": entry.get("credit_source", "local"),
                "last_sync": 0,
                "role": entry.get("role", "worker"),
                "renewal": entry.get("renewal") or PROVIDER_SPECS[prov]["renewal"],
            }
            _keys.append(full)
            existing_keys.add(full["key"]); existing_labels.add(final)
            added += 1
            with _key_sems_lock:
                _key_sems[final] = threading.Semaphore(KEY_CONCURRENCY_LIMITS.get(prov, 1))
    _save_keys()
    return {"status": "ok", "added": added, "skipped": skipped}

@app.delete("/keys/remove")
async def remove_key(req: RemoveKeyRequest):
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "DELETE", "/keys/remove", req.dict())
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")
    with _keys_lock:
        if req.key:
            entry = next((k for k in _keys if k["key"] == req.key), None)
        elif req.label:
            entry = next((k for k in _keys if k["label"] == req.label), None)
        else:
            raise HTTPException(400, "key or label required")
        if not entry: raise HTTPException(404, "not found")
        label = entry["label"]
        _keys[:] = [k for k in _keys if k["label"] != label]
    with _key_sems_lock: _key_sems.pop(label, None)
    _save_keys()
    return {"status": "ok", "removed": label}

@app.get("/keys/list")
async def list_keys():
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "GET", "/keys/list")
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")
    with _keys_lock:
        return [{"label": k["label"], "provider": k["provider"],
                 "role": k.get("role", "worker"),
                 "renewal": k.get("renewal", "unknown"),
                 "hint": f"***{k['key'][-6:]}" if k.get("key") else ""}
                for k in _keys]

@app.get("/keys/stats")
async def keys_stats():
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "GET", "/keys/stats")
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")

    with _keys_lock:
        keys = [dict(k) for k in _keys]

    by_prov = defaultdict(lambda: {"keys": 0, "live": 0,
                                    "total_credits": 0, "used_credits": 0,
                                    "renewal": "unknown"})
    out = []
    for k in keys:
        prov  = k.get("provider", "")
        limit = int(k.get("credits_limit") or DEFAULT_CREDIT_LIMITS.get(prov, 0))
        used  = int(k.get("credits_used") or 0)
        live  = used < limit
        out.append({
            "label": k.get("label"), "provider": prov,
            "role": k.get("role", "worker"),
            "renewal": k.get("renewal", "unknown"),
            "credits_limit": limit, "credits_used": used, "live": live,
            "reset_at": k.get("reset_at", 0),
            "reset_at_override": k.get("reset_at_override", 0),
            "reset_interval_days": k.get("reset_interval_days", 30),
            "last_used": k.get("last_used", 0),
            "credit_source": k.get("credit_source", "local"),
            "last_sync": k.get("last_sync", 0),
            "hint": f"***{k['key'][-6:]}" if k.get("key") else "",
        })
        b = by_prov[prov]
        b["keys"] += 1
        b["total_credits"] += limit
        b["used_credits"]  += used
        b["renewal"] = PROVIDER_SPECS.get(prov, {}).get("renewal", "unknown")
        if live: b["live"] += 1
    return {"providers": dict(by_prov), "keys": out}

@app.post("/keys/sync")
async def keys_sync(x_secret: Optional[str] = Header(None)):
    if ROLE == "dashboard":
        code, body = await asyncio.get_event_loop().run_in_executor(
            _api_executor, _vps_forward, "POST", "/keys/sync")
        if code >= 400: raise HTTPException(code, body.get("error", "vps error"))
        return body
    if ROLE != "vps": raise HTTPException(400, "keys only on vps/dashboard")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_api_executor, _sync_all_keys)

@app.post("/notify/test")
async def notify_test(x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET: raise HTTPException(403, "invalid secret")
    if not NTFY_ENABLED: raise HTTPException(400, "NTFY_TOPIC not set")
    notify("✅ Test notification",
           f"ProxyStack {ROLE} — ntfy working",
           "default", ["white_check_mark"])
    return {"sent": True, "topic": NTFY_TOPIC}

@app.get("/")
def root():
    if ROLE in ("pool", "dashboard"):
        dash = BASE / "static" / "dashboard.html"
        if dash.exists(): return HTMLResponse(dash.read_text())
        return HTMLResponse("<h2>ProxyStack v2</h2>"
                            "<p><a href='/stats'>stats</a></p>")
    return {"role": ROLE, "status": "running", "notify": NTFY_ENABLED}