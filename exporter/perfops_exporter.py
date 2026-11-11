import argparse
import math
import requests
import json
import os
import sys
import time
import hashlib
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Configuration ---
API_KEY = os.environ.get("PERFOPS_API_KEY")
BASE_URL = "https://api.perfops.net/analytics/cdn/raw-logs"
PROVIDERS_URL = "https://api.perfops.net/analytics/cdn/provider"
PROVIDER_REFRESH = int(os.environ.get("PROVIDER_REFRESH", "3600"))
HEADERS = {
    "Authorization": f"{API_KEY}",
    "Content-Type": "application/json"
}
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", "120"))

# InfluxDB Configuration
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "perfops")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "perfops-cdn")
INFLUX_BATCH_SIZE = int(os.environ.get("INFLUX_BATCH_SIZE", "5000"))
# Write timeout (the client default is 10s, which is what tripped during the Nov 3-4 overload)
INFLUX_TIMEOUT_MS = int(os.environ.get("INFLUX_TIMEOUT_MS", "30000"))
# Retries per batch for transient write errors (timeouts, connection refused, 429/5xx)
INFLUX_WRITE_RETRIES = int(os.environ.get("INFLUX_WRITE_RETRIES", "5"))

# --- Geocoding configuration ---
GEO_CACHE_FILE = os.environ.get("GEO_CACHE_FILE", "geo_cache.json")
GEOCODER_USER_AGENT = os.environ.get("GEOCODER_USER_AGENT", "perfops-exporter (ops@example.com)")

# --- Backfill & Pagination configuration ---
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", "/data/checkpoint.json")
MAX_BACKFILL_HOURS = int(os.environ.get("MAX_BACKFILL_HOURS", "48"))
RETENTION_SAFETY_MINUTES = int(os.environ.get("RETENTION_SAFETY_MINUTES", "5"))
BACKFILL_CHUNK_MINUTES = int(os.environ.get("BACKFILL_CHUNK_MINUTES", "2"))
OVERLAP_SECONDS = int(os.environ.get("OVERLAP_SECONDS", "60"))
INGEST_LAG_SECONDS = int(os.environ.get("INGEST_LAG_SECONDS", "0"))
INITIAL_BACKFILL_HOURS = int(os.environ.get("INITIAL_BACKFILL_HOURS", "0"))
BACKFILL_REQUEST_DELAY = float(os.environ.get("BACKFILL_REQUEST_DELAY", "0.2"))
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "10000"))
MAX_PAGES_PER_WINDOW = int(os.environ.get("MAX_PAGES_PER_WINDOW", "100"))
CDN_FILTER = os.environ.get("CDN_FILTER", "").strip()
# Network diagnosis data (added 2026-11-09), from the raw-logs asn / isp / state / clientDns /
# reqSeqNumber columns. NETWORK_TAGS=on (default) or off (original schema).
#   tags   : asn, isp, state, resolver_class (isp/public/other/none/unknown),
#            resolver_remote (yes/no/anycast/unknown)
#            asn/isp/state are (nearly) determined by city, so they add few series;
#            resolver_class/resolver_remote have at most 5 and 4 values.
#   fields : as_name, resolver_asn, resolver_country, resolver_as_name, resolver_km,
#            resolver_count, req_seq   (fields, NOT tags: they would multiply raw-bucket series)
# See network-diag-runbook.md for the cardinality check after deploy.
NETWORK_TAGS = os.environ.get("NETWORK_TAGS", "on").strip().lower()
if NETWORK_TAGS not in ("on", "off"):
    print(f"WARNING: NETWORK_TAGS='{NETWORK_TAGS}' is not on/off; using 'on'.")
    NETWORK_TAGS = "on"

# Resolver ASNs counted as public DNS services (resolver_class="public")
PUBLIC_RESOLVER_ASNS = {
    15169: "Google", 13335: "Cloudflare", 19281: "Quad9", 36692: "Cisco OpenDNS",
    30607: "Cisco OpenDNS", 212772: "AdGuard", 34939: "NextDNS", 13238: "Yandex",
    23393: "Neustar UltraDNS public", 42: "PCH (Quad9)", 3356: "Level 3",
}

# --- CDN comparison collector (added 2026-11-11) ---------------------------------------
# Hourly per-CDN aggregates from /analytics/cdn/data (PerfOps' own pre-aggregated stats,
# no raw logs), written to COMPARE_BUCKET as measurement perfops_cdn_compare:
#   tags   provider_name, country (name, or "_all"), asn (number, or "_all")
#   fields mean_ms, median_ms, p95_ms   request time (ms = ttfb + transfer; excludes setup)
#          tcp_ms, tls_ms      connection setup AVERAGED OVER ALL REQUESTS (reused
#                              connections count as 0); tcp includes TLS.
#                              mean_ms + tcp_ms = mean time per request incl. setup (no DNS)
#                              tls / (tcp - tls) = TLS round trips per TCP round trip.
#          ttfb_ms, uptime     (country / global rows only)
# Verified 2026-11-11 against raw logs (F5, AS7922, one hour): API median 27 = median(ms) 27,
# mean 37.0 ~ 37.7, ttfb 35.5 ~ 35.6, tcptime 31.2 ~ sum(tcp)/all requests 33.4.
# Three levels per hour: country x ASN (per CDN, limited to the anchor CDN's country x ASN
# pairs), country (all CDNs, one call) and global (one call). ~8 API calls per hour.
COMPARE = os.environ.get("COMPARE", "off").strip().lower() == "on"
COMPARE_CDNS = os.environ.get("COMPARE_CDNS",
    "F5 CDN,Akamai CDN,Cloudflare CDN,Fastly CDN,AWS CloudFront CDN,Google Cloud CDN").strip()
COMPARE_ANCHOR = os.environ.get("COMPARE_ANCHOR", "F5 CDN").strip()
# ASN-level rows only for these countries (names or ISO codes, comma-separated; empty = all)
COMPARE_COUNTRIES = os.environ.get("COMPARE_COUNTRIES", "").strip()
COMPARE_BUCKET = os.environ.get("COMPARE_BUCKET", "perfops-cdn-1h")
COMPARE_LAG_MINUTES = int(os.environ.get("COMPARE_LAG_MINUTES", "30"))
COMPARE_INITIAL_HOURS = int(os.environ.get("COMPARE_INITIAL_HOURS", "24"))
COMPARE_MAX_HOURS_PER_CYCLE = int(os.environ.get("COMPARE_MAX_HOURS_PER_CYCLE", "3"))
COMPARE_CHECKPOINT_FILE = os.environ.get("COMPARE_CHECKPOINT_FILE", "/data/compare_checkpoint.json")
DATA_URL = "https://api.perfops.net/analytics/cdn/data"
COUNTRIES_URL = "https://api.perfops.net/analytics/cdn/countries"
COMPARE_FUNCS = {"MEAN": "mean_ms", "MEDIAN": "median_ms", "PERCENTILE_95": "p95_ms", "TCPTIME": "tcp_ms",
                 "TLSTIME": "tls_ms", "TTFB": "ttfb_ms", "UPTIME": "uptime"}
COMPARE_ASN_FUNCS = "mean,median,percentile_95,tcptime,tlstime"
COMPARE_AGG_FUNCS = "mean,median,percentile_95,tcptime,tlstime,ttfb,uptime"

# When a window hits MAX_PAGES_PER_WINDOW it is split into smaller windows and
# re-fetched, down to this size. Below it, the truncation is logged and accepted.
MIN_SPLIT_SECONDS = int(os.environ.get("MIN_SPLIT_SECONDS", "10"))

# fetch_window() results
FETCH_OK, FETCH_TRUNCATED, FETCH_FAILED = "ok", "truncated", "failed"

GEO_OVERRIDES = {
    "Ashburn|United States":  [39.0438, -77.4874],
    "Pasco|United States":    [46.2396, -119.1006],
    "Suffolk|United States":  [36.7282, -76.5836],
    "Alameda|United States":  [37.7652, -122.2416],
    "Dnipro|Ukraine":         [48.4647, 35.0462],
    "Ain Beida|Algeria":      [35.7964, 7.3928],
}

_geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT, timeout=10)
_geocode = RateLimiter(_geolocator.geocode, min_delay_seconds=1.1,
                       max_retries=2, error_wait_seconds=5)

def load_geo_cache():
    try:
        with open(GEO_CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

geo_cache = load_geo_cache()

def save_geo_cache():
    tmp = f"{GEO_CACHE_FILE}.{os.getpid()}.tmp"
    os.makedirs(os.path.dirname(os.path.abspath(tmp)), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(geo_cache, f)
    os.replace(tmp, GEO_CACHE_FILE)

def get_city_coords(city, country):
    if not city or city in ("unknown", "None", "?", ""):
        return None

    key = f"{city}|{country}"
    if key in GEO_OVERRIDES:
        return GEO_OVERRIDES[key]

    if key in geo_cache:
        return geo_cache[key]

    kwargs = {}
    if len(country) == 2 and country.isalpha():
        kwargs["country_codes"] = country.lower()
        query = city
    elif country and country != "unknown":
        query = f"{city}, {country}"
    else:
        query = city

    try:
        loc = _geocode(query, **kwargs)
    except Exception as e:
        print(f"Geocoding failed for '{query}': {e}")
        return None

    coords = [round(loc.latitude, 4), round(loc.longitude, 4)] if loc else None
    if coords is None:
        print(f"No geocoding result for '{query}' ({country})")
    geo_cache[key] = coords
    save_geo_cache()
    return coords

provider_map = {}
providers_loaded_at = 0.0
cdn_filter_ids = []

def load_providers():
    global provider_map, providers_loaded_at, cdn_filter_ids
    print("Loading CDN providers...")
    try:
        response = requests.get(PROVIDERS_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        new_map = {str(p.get('id')): p.get('name') or f"Provider_{p.get('id')}"
                   for p in response.json()}
        if new_map:
            provider_map = new_map
            providers_loaded_at = time.time()
        print(f"Loaded {len(provider_map)} CDN providers.")
        
        # Resolve CDN_FILTER
        if CDN_FILTER:
            name_to_id = {v.lower(): k for k, v in provider_map.items()}
            new_filter_ids = []
            for name in CDN_FILTER.split(","):
                n = name.strip().lower()
                if n in name_to_id:
                    new_filter_ids.append(name_to_id[n])
                elif n:
                    print(f"WARNING: CDN name '{name.strip()}' not found in provider list. "
                          f"Available: {sorted(provider_map.values())}")
            
            if new_filter_ids:
                cdn_filter_ids = new_filter_ids
                print(f"CDN_FILTER active: '{CDN_FILTER}' → IDs: {cdn_filter_ids}")
            else:
                # Do NOT fall back to all CDNs: an unfiltered fetch overflows
                # MAX_PAGES_PER_WINDOW and writes other providers' data.
                print(f"ERROR: CDN_FILTER is set to '{CDN_FILTER}' but no matching CDNs found. "
                      f"Fetching is paused until the filter resolves.")
    except Exception as e:
        print(f"Warning: Could not load CDN providers list: {e}")

def filter_ready():
    """False until the provider list is loaded (otherwise every row is tagged
    provider_name="CDN_<id>") and, if CDN_FILTER is set, resolved to provider IDs."""
    return bool(provider_map) and (not CDN_FILTER or bool(cdn_filter_ids))

def ensure_providers(max_attempts=None, wait_seconds=30):
    """Load providers until CDN_FILTER resolves. max_attempts=None tries once."""
    attempts = 0
    while True:
        if not provider_map or not filter_ready():
            load_providers()
        if filter_ready():
            return True
        attempts += 1
        if max_attempts is None or attempts >= max_attempts:
            return False
        print(f"CDN_FILTER not resolved yet; retrying in {wait_seconds}s "
              f"(attempt {attempts}/{max_attempts})...")
        time.sleep(wait_seconds)

def get_provider_name(cdnid):
    return provider_map.get(str(cdnid), f"CDN_{cdnid}")

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
            if "last_end_time" in data:
                return datetime.fromisoformat(data["last_end_time"])
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return None

def save_checkpoint(end_time: datetime):
    tmp = f"{CHECKPOINT_FILE}.{os.getpid()}.tmp"
    os.makedirs(os.path.dirname(os.path.abspath(tmp)), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump({"last_end_time": end_time.isoformat()}, f)
    os.replace(tmp, CHECKPOINT_FILE)

def extract_timestamp(row, col_idx, fallback_ns):
    possible_cols = ['timestamp', 'datetime', 'dateTime', 'date', 'time', 'ts', 'created', 'createdAt']
    for col in possible_cols:
        if col in col_idx:
            val = row[col_idx[col]]
            if val is not None:
                if isinstance(val, (int, float)):
                    if val > 100000000000:  # ms
                        return int(val * 1000000) if isinstance(val, int) else int(val * 1000000.0)
                    else:  # s
                        return int(val * 1000000000) if isinstance(val, int) else int(val * 1000000000.0)
                elif isinstance(val, str):
                    try:
                        # Try parsing as ISO
                        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return int(dt.timestamp() * 1000000000)
                    except ValueError:
                        try:
                            # Try YYYY-MM-DD HH:MM:SS
                            dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                            return int(dt.timestamp() * 1000000000)
                        except ValueError:
                            pass
    return fallback_ns

# --- Network diagnosis helpers ---------------------------------------------------------
_RESOLVER_KEYS = ["ip", "asn", "asnName", "country", "continent", "city", "state",
                  "latitude", "longitude"]          # list order in the raw-logs example

def _num(v):
    """int/float/numeric string -> float, else None"""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None

def _clean_tag(v, maxlen=64):
    """Tag-safe string: never empty, no newlines, bounded length."""
    if v is None:
        return "unknown"
    s = " ".join(str(v).split())[:maxlen]
    return s if s and s not in ("?", "None", "null") else "unknown"

def _asn_number(v):
    """asn can be {"number": 7922, "name": "..."} or a bare number/string"""
    if isinstance(v, dict):
        v = v.get("number")
    n = _num(v)
    return int(n) if n is not None and n > 0 else None

def parse_resolvers(raw):
    """clientDns -> list of dicts. Accepts a list of dicts (keys as in the API docs) or a list
    of lists in the documented order: ip, asn, asnName, country, continent, city, state, lat, lon."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    if isinstance(raw, dict):
        raw = [raw]
    out = []
    for item in raw or []:
        if isinstance(item, dict):
            d = dict(item)
            if "lat" in d and "latitude" not in d:
                d["latitude"] = d.get("lat")
            if "lon" in d and "longitude" not in d:
                d["longitude"] = d.get("lon")
        elif isinstance(item, (list, tuple)):
            d = dict(zip(_RESOLVER_KEYS, item))
        else:
            continue
        out.append(d)
    return out

def _is_local_ip(ip):
    ip = str(ip or "")
    return (ip.startswith("127.") or ip == "::1" or ip.startswith("10.") or
            ip.startswith("192.168.") or ip.startswith("169.254.") or
            any(ip.startswith(f"172.{i}.") for i in range(16, 32)))

def pick_resolver(resolvers):
    """The resolver that faced the internet: first entry with a non-local IP and an ASN,
    else the first entry with an ASN, else the first entry."""
    for r in resolvers:
        if not _is_local_ip(r.get("ip")) and _asn_number(r.get("asn")):
            return r
    for r in resolvers:
        if _asn_number(r.get("asn")):
            return r
    return resolvers[0] if resolvers else None

def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))

def add_network_info(point, get_val, country, coords):
    """Adds the network-diagnosis tags/fields (see NETWORK_TAGS) to a point."""
    if NETWORK_TAGS == "off":
        return point

    asn_raw = get_val('asn')
    asn = _asn_number(asn_raw)
    as_name = asn_raw.get("name") if isinstance(asn_raw, dict) else None
    point = point.tag("asn", str(asn) if asn else "unknown") \
                 .tag("isp", _clean_tag(get_val('isp')))
    if as_name:
        point = point.field("as_name", str(as_name)[:128])

    resolvers = parse_resolvers(get_val('clientDns'))
    res = pick_resolver(resolvers)
    res_asn = _asn_number(res.get("asn")) if res else None
    res_country = _clean_tag(res.get("country")) if res else "unknown"

    # Public resolvers (Google, Cloudflare, ...) are anycast: the location the API reports for
    # them (e.g. 8.8.8.8 -> Mountain View) is not where the query was answered, so distance
    # and "remote" are meaningless for them.
    if not res:
        res_class = "none"
    elif res_asn and asn and res_asn == asn:
        res_class = "isp"                     # the probe's own ISP resolver
    elif res_asn in PUBLIC_RESOLVER_ASNS:
        res_class = "public"                  # Google, Cloudflare, Quad9, ...
    elif res_asn:
        res_class = "other"                   # a third-party / corporate resolver
    else:
        res_class = "unknown"

    if res_class == "public":
        res_remote = "anycast"
    elif res_country == "unknown" or country in ("unknown", "", "None"):
        res_remote = "unknown"
    else:
        res_remote = "no" if res_country == country else "yes"

    point = point.tag("state", _clean_tag(get_val('state'))) \
                 .tag("resolver_class", res_class) \
                 .tag("resolver_remote", res_remote)

    point = point.field("resolver_count", float(len(resolvers)))
    if res:
        if res_asn:
            point = point.field("resolver_asn", str(res_asn))
        if res_country != "unknown":
            point = point.field("resolver_country", res_country)
        name = res.get("asnName")
        if name:
            point = point.field("resolver_as_name", str(name)[:128])
        rlat, rlon = _num(res.get("latitude")), _num(res.get("longitude"))
        if (res_class != "public" and coords and rlat is not None and rlon is not None
                and not (rlat == 0 and rlon == 0)):
            point = point.field("resolver_km",
                                round(haversine_km(coords[0], coords[1], rlat, rlon), 1))

    seq = _num(get_val('reqSeqNumber'))
    if seq is not None:
        point = point.field("req_seq", seq)
    return point

def build_point(row, col_idx, fallback_ns):
    def get_val(col_name, default=None):
        if col_name in col_idx:
            idx = col_idx[col_name]
            if idx < len(row):
                return row[idx]
        return default

    cdnid = get_val('cdnid')
    if not cdnid:
        return None
        
    provider_name = get_provider_name(cdnid)
    continent = str(get_val('continent', 'unknown'))
    country = str(get_val('country', 'unknown'))
    city = str(get_val('city', 'unknown'))
    platform = str(get_val('platform', 'unknown'))
    http_version = str(get_val('httpVersion', 'unknown'))
    
    cache = get_val('cache')
    # The API documents cache values 1 (hit) and -1 (miss); 0 is also treated as a miss.
    cache_status = 'hit' if cache == 1 else 'miss' if cache in (0, -1) else str(cache)
    status_code = str(get_val('statusCode', 'unknown'))
    failure_reason = str(get_val('failureReason', 'NONE'))

    point = Point("perfops_cdn_logs") \
        .tag("provider_name", provider_name) \
        .tag("continent", continent) \
        .tag("country", country) \
        .tag("city", city) \
        .tag("platform", platform) \
        .tag("httpVersion", http_version) \
        .tag("cache_status", cache_status) \
        .tag("status_code", status_code) \
        .tag("failure_reason", failure_reason)

    ttfb = get_val('ttfb')
    if isinstance(ttfb, (int, float)) and ttfb > 0:
        point = point.field("ttfb_ms", float(ttfb))
        
    dns = get_val('dnsLookupTimeMs')
    if isinstance(dns, (int, float)) and dns >= 0:
        point = point.field("dns_lookup_ms", float(dns))
        
    tcp = get_val('tcpTimeMs')
    if isinstance(tcp, (int, float)) and tcp >= 0:
        point = point.field("tcp_connect_ms", float(tcp))
        
    ssl = get_val('sslTimeMs')
    if isinstance(ssl, (int, float)) and ssl >= 0:
        point = point.field("ssl_handshake_ms", float(ssl))
        
    transfer = get_val('transferTime')
    if isinstance(transfer, (int, float)) and transfer >= 0:
        point = point.field("transfer_ms", float(transfer))
        
    total_ms = get_val('ms')
    if isinstance(total_ms, (int, float)) and total_ms > 0:
        point = point.field("total_latency_ms", float(total_ms))

    api_lat = get_val('latitude', get_val('lat'))
    api_lon = get_val('longitude', get_val('lon'))
    if isinstance(api_lat, (int, float)) and isinstance(api_lon, (int, float)):
        coords = [float(api_lat), float(api_lon)]
    else:
        coords = get_city_coords(city, country)

    if coords:
        point = point.field("lat", float(coords[0])).field("lon", float(coords[1]))

    point = add_network_info(point, get_val, country, coords)

    ts_ns = extract_timestamp(row, col_idx, fallback_ns)
    
    # Hash row to create stable sub-second offset to avoid duplicates
    row_str = json.dumps(row, sort_keys=True)
    hash_val = int(hashlib.md5(row_str.encode()).hexdigest(), 16)
    offset_ns = hash_val % 1000000 # up to 1ms offset
    
    point = point.time(ts_ns + offset_ns, WritePrecision.NS)
    return point

def write_with_retry(write_api, batch):
    """Write one batch, retrying transient errors with exponential backoff.
    Re-writing the same points is safe: identical series + timestamp overwrite."""
    for attempt in range(1, INFLUX_WRITE_RETRIES + 1):
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=batch)
            return True
        except ApiException as e:
            # 4xx other than 429 means bad data/auth: retrying won't help
            if e.status and 400 <= e.status < 500 and e.status != 429:
                print(f"InfluxDB write failed ({e.status}): {e.body}")
                return False
            err = f"HTTP {e.status}"
        except Exception as e:
            err = str(e)
        if attempt == INFLUX_WRITE_RETRIES:
            print(f"InfluxDB write failed after {attempt} attempts: {err}")
            return False
        wait = min(60, 5 * 2 ** (attempt - 1))
        print(f"InfluxDB write error (attempt {attempt}/{INFLUX_WRITE_RETRIES}): {err}. "
              f"Retrying in {wait}s...")
        time.sleep(wait)
    return False

def fetch_window(write_api, start_time: datetime, end_time: datetime):
    """Fetch one window and write it. Returns (status, info):
    FETCH_OK, FETCH_FAILED, or FETCH_TRUNCATED with info = (rows, min_ts_ns, max_ts_ns)
    describing what was covered before MAX_PAGES_PER_WINDOW was reached."""
    date_from = start_time.strftime('%Y-%m-%d %H:%M:%S')
    date_to = end_time.strftime('%Y-%m-%d %H:%M:%S')
    fallback_ns = int(end_time.timestamp() * 1e9)
    
    page = 1
    pages_fetched = 0
    total_points = 0
    logged_missing_ts = False
    prev_page_hash = None
    min_ts_ns = None
    max_ts_ns = None
    hit_max_pages = False
    
    while page <= MAX_PAGES_PER_WINDOW:
        params = {
            "dateTimeFrom": date_from,
            "dateTimeTo": date_to,
            "page": page,
            "pagelimit": PAGE_LIMIT
        }
        if cdn_filter_ids:
            params["providers"] = ",".join(cdn_filter_ids)
            
        retry_count = 0
        while retry_count < 3:
            try:
                response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
                if response.status_code == 429:
                    retry_after_str = response.headers.get("Retry-After", "5")
                    try:
                        retry_after = int(retry_after_str)
                    except ValueError:
                        from email.utils import parsedate_to_datetime
                        try:
                            dt = parsedate_to_datetime(retry_after_str)
                            retry_after = max(0, int((dt - datetime.now(timezone.utc)).total_seconds()))
                        except (TypeError, ValueError):
                            retry_after = 5
                    print(f"Rate limited, retrying after {retry_after}s...")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from PerfOps API: {e} | Body: {getattr(e.response, 'text', '')}")
                retry_count += 1
                if retry_count >= 3:
                    return FETCH_FAILED, None
                time.sleep(2 ** retry_count)
                
        if retry_count >= 3:
            return FETCH_FAILED, None

        body = response.json()
        result_array = body.get('result', [])
        if not result_array or 'data' not in result_array[0] or not result_array[0]['data']:
            break
            
        data_block = result_array[0]['data'][0]
        columns = data_block.get('columns', [])
        values = data_block.get('values', [])

        if not columns or not values:
            break

        # Detect if the API is ignoring `page` and returning identical rows
        page_hash = hashlib.md5(json.dumps(values, sort_keys=True).encode()).hexdigest()
        if page_hash == prev_page_hash:
            print(f"WARNING: Page {page} returned identical data to page {page - 1}. "
                  f"API may be ignoring pagination parameters. Stopping early.")
            break
        prev_page_hash = page_hash

        col_idx = {name: idx for idx, name in enumerate(columns)}
        points = []
        for row in values:
            row_ts = extract_timestamp(row, col_idx, -1)
            if row_ts != -1:
                min_ts_ns = row_ts if min_ts_ns is None else min(min_ts_ns, row_ts)
                max_ts_ns = row_ts if max_ts_ns is None else max(max_ts_ns, row_ts)
            pt = build_point(row, col_idx, fallback_ns)
            if pt:
                points.append(pt)
                
        if not logged_missing_ts and len(points) > 0:
            if extract_timestamp(values[0], col_idx, -1) == -1:
                print(f"Warning: No valid timestamp column found. Available columns: {columns}")
                logged_missing_ts = True
                
        if points:
            # Write in batches of INFLUX_BATCH_SIZE (default 5000)
            for i in range(0, len(points), INFLUX_BATCH_SIZE):
                batch = points[i:i + INFLUX_BATCH_SIZE]
                if not write_with_retry(write_api, batch):
                    return FETCH_FAILED, None
            total_points += len(points)
            pages_fetched += 1

        if len(values) < PAGE_LIMIT:
            break
            
        if page == MAX_PAGES_PER_WINDOW:
            hit_max_pages = True
            break

        page += 1
        if BACKFILL_REQUEST_DELAY > 0:
            time.sleep(BACKFILL_REQUEST_DELAY)

    ts_range_info = ""
    if min_ts_ns and max_ts_ns:
        min_dt = datetime.fromtimestamp(min_ts_ns / 1e9, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        max_dt = datetime.fromtimestamp(max_ts_ns / 1e9, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ts_range_info = f" [actual data ts: {min_dt} .. {max_dt}]"

    print(f"Successfully processed {total_points} points from {date_from} to {date_to} "
          f"(pages: {pages_fetched}){ts_range_info}")
    if hit_max_pages:
        print(f"WARNING: Hit MAX_PAGES_PER_WINDOW ({MAX_PAGES_PER_WINDOW}) for {date_from} to {date_to}! "
              f"Data for this window was truncated.")
        return FETCH_TRUNCATED, (total_points, min_ts_ns, max_ts_ns)
    return FETCH_OK, None

def fetch_range(write_api, start_time: datetime, end_time: datetime):
    """Fetch [start_time, end_time); if the API page cap truncates it, split the
    window into smaller ones and fetch those, so no rows are dropped.
    Returns True on success, False on failure."""
    status, info = fetch_window(write_api, start_time, end_time)
    if status == FETCH_OK:
        return True
    if status == FETCH_FAILED:
        return False

    span = (end_time - start_time).total_seconds()
    if span < 2 * MIN_SPLIT_SECONDS:
        print(f"WARNING: window {start_time} to {end_time} is still truncated at the minimum split size "
              f"({MIN_SPLIT_SECONDS}s); accepting partial data. Consider raising PAGE_LIMIT/MAX_PAGES_PER_WINDOW.")
        return True

    # Estimate how many sub-windows are needed from the density of what we got
    # (target 70% of the page cap per sub-window). Fall back to halving.
    rows, min_ts, max_ts = info
    parts = 2
    if rows and min_ts and max_ts and max_ts > min_ts:
        rate = rows / ((max_ts - min_ts) / 1e9)                 # rows per second
        capacity = PAGE_LIMIT * MAX_PAGES_PER_WINDOW * 0.7
        parts = max(2, math.ceil(rate * span / capacity))
    parts = min(parts, int(span // MIN_SPLIT_SECONDS))
    step = math.ceil(span / parts)                              # whole seconds (API resolution)
    print(f"Splitting {start_time} to {end_time} into {parts} sub-windows of ~{step}s to recover truncated rows.")

    sub_start = start_time
    while sub_start < end_time:
        sub_end = min(sub_start + timedelta(seconds=step), end_time)
        if not fetch_range(write_api, sub_start, sub_end):
            return False
        sub_start = sub_end
    return True

def catch_up(write_api):
    if not filter_ready():
        print(f"Skipping fetch: provider list not loaded / CDN_FILTER '{CDN_FILTER}' not resolved yet.")
        return
    now = datetime.now(timezone.utc) - timedelta(seconds=INGEST_LAG_SECONDS)
    earliest = now - timedelta(hours=MAX_BACKFILL_HOURS) + timedelta(minutes=RETENTION_SAFETY_MINUTES)
    
    checkpoint = load_checkpoint()
    
    if not checkpoint:
        start = now - timedelta(hours=INITIAL_BACKFILL_HOURS) if INITIAL_BACKFILL_HOURS > 0 else now - timedelta(seconds=SCRAPE_INTERVAL)
    else:
        start = max(checkpoint - timedelta(seconds=OVERLAP_SECONDS), earliest)
        if checkpoint < earliest:
            print(f"WARNING: unrecoverable gap from {checkpoint} to {earliest}. Outage exceeded {MAX_BACKFILL_HOURS} hours.")
            
    chunk_delta = timedelta(minutes=BACKFILL_CHUNK_MINUTES)

    while start < now:
        end = min(start + chunk_delta, now)
        if (end - start).total_seconds() < 5:
            break

        success = fetch_range(write_api, start, end)
        if not success:
            print(f"Failed to process chunk from {start} to {end}. Stopping catch-up for this cycle.")
            break
        
        save_checkpoint(end)
        start = end
        
        now = datetime.now(timezone.utc) - timedelta(seconds=INGEST_LAG_SECONDS)
        if start < now and BACKFILL_REQUEST_DELAY > 0:
            time.sleep(BACKFILL_REQUEST_DELAY)

def parse_utc(value: str) -> datetime:
    """Accepts '2026-11-03T16:59:37Z', '2026-11-03 16:59:37' etc. Naive values are UTC."""
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def refetch(write_api, start: datetime, end: datetime, max_window_retries=5):
    """One-shot re-fetch of [start, end). Never reads or writes the checkpoint, so it
    can run alongside the live exporter. Safe to repeat: rows get the same timestamp
    (row-hash offset) and overwrite instead of duplicating."""
    chunk = timedelta(minutes=BACKFILL_CHUNK_MINUTES)
    total_windows = math.ceil((end - start) / chunk)
    done = 0
    t0 = time.time()
    cur = start
    while cur < end:
        nxt = min(cur + chunk, end)
        for attempt in range(1, max_window_retries + 1):
            if fetch_range(write_api, cur, nxt):
                break
            wait = min(300, 30 * attempt)
            print(f"Window {cur} to {nxt} failed (attempt {attempt}/{max_window_retries}); retrying in {wait}s")
            time.sleep(wait)
        else:
            print(f"ERROR: giving up at {cur}. Resume with:\n"
                  f"  python perfops_exporter.py --refetch-start '{cur.isoformat()}' --refetch-end '{end.isoformat()}'")
            return False
        done += 1
        if done % 30 == 0 or nxt >= end:
            rate = (nxt - start).total_seconds() / max(1.0, time.time() - t0)
            print(f"[refetch] {done}/{total_windows} windows done, now at {nxt} ({rate:.1f}x real time)")
        cur = nxt
        if cur < end and BACKFILL_REQUEST_DELAY > 0:
            time.sleep(BACKFILL_REQUEST_DELAY)
    print(f"[refetch] complete: {start} to {end}")
    return True

# --- CDN comparison collector ------------------------------------------------------------
compare_countries = {}          # PerfOps country id (str) -> name
compare_country_ids = []        # COMPARE_COUNTRIES resolved to ids

def api_get(url, params, timeout=120, attempts=4):
    """GET with retry on 429 / 5xx / network errors. Returns parsed JSON or None."""
    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                wait = int(r.headers.get("Retry-After", "0") or 0) or min(120, 10 * attempt)
                print(f"[compare] HTTP {r.status_code} from {url}; retry in {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                print(f"[compare] HTTP {r.status_code} from {url}: {r.text[:300]}")
                return None
            return r.json()
        except Exception as e:
            print(f"[compare] request error ({attempt}/{attempts}): {e}")
            time.sleep(min(120, 10 * attempt))
    return None

def load_compare_countries():
    global compare_countries, compare_country_ids
    data = api_get(COUNTRIES_URL, {}, timeout=30)
    if not data:
        return False
    compare_countries = {str(c["id"]): c.get("name") or str(c["id"]) for c in data}
    ids = []
    if COMPARE_COUNTRIES:
        by_name = {}
        for c in data:
            by_name[str(c.get("name", "")).lower()] = str(c["id"])
            by_name[str(c.get("iso", "")).lower()] = str(c["id"])
        for n in COMPARE_COUNTRIES.split(","):
            cid = by_name.get(n.strip().lower())
            if cid:
                ids.append(cid)
            elif n.strip():
                print(f"[compare] WARNING: country '{n.strip()}' not found")
    compare_country_ids = ids
    return True

def compare_provider_ids():
    """COMPARE_CDNS names -> PerfOps provider ids (anchor first). Unknown names are skipped."""
    name_to_id = {v.lower(): k for k, v in provider_map.items()}
    names = [COMPARE_ANCHOR] + [n.strip() for n in COMPARE_CDNS.split(",")
                                if n.strip() and n.strip().lower() != COMPARE_ANCHOR.lower()]
    ids = []
    for n in names:
        pid = name_to_id.get(n.lower())
        if pid:
            ids.append(pid)
        else:
            print(f"[compare] WARNING: CDN '{n}' not in provider list")
    return ids

def _compare_rows(body):
    """{(cdnid, country_id|'_all', asn|'_all'): {field: value}} from a /cdn/data response."""
    out = {}
    for res in (body or {}).get("result", []):
        field = COMPARE_FUNCS.get(str(res.get("statFunction", "")).upper())
        if not field:
            continue
        for x in res.get("data", []):
            y = x.get("y")
            if y is None or x.get("cdnid") is None:
                continue
            key = (str(x["cdnid"]), str(x.get("country", "_all")), str(x.get("asn", "_all")))
            out.setdefault(key, {})[field] = float(y)
    return out

def compare_hour(write_api, hour_start):
    """Collect and write one hour. Returns True on success."""
    f = '%Y-%m-%d %H:%M:%S'
    base = {"dateTimeFrom": hour_start.strftime(f),
            "dateTimeTo": (hour_start + timedelta(hours=1)).strftime(f),
            "groupByTime": "hour"}
    if compare_country_ids:
        base["countries"] = ",".join(compare_country_ids)
    ids = compare_provider_ids()
    if not ids or provider_map.get(ids[0], "").lower() != COMPARE_ANCHOR.lower():
        print(f"[compare] anchor CDN '{COMPARE_ANCHOR}' not resolved; skipping")
        return False

    rows = {}
    # 1. country x ASN, one CDN per call; competitors limited to the anchor's pairs
    anchor_pairs = None
    for pid in ids:
        body = api_get(DATA_URL, dict(base, providers=pid, groupBase="country,asn",
                                      statFunction=COMPARE_ASN_FUNCS))
        if body is None:
            return False
        r = _compare_rows(body)
        if anchor_pairs is None:
            anchor_pairs = {(k[1], k[2]) for k in r}
        else:
            r = {k: v for k, v in r.items() if (k[1], k[2]) in anchor_pairs}
        rows.update(r)
    # 2. per country and 3. global, all CDNs in one call each
    for gb in ("country", "provider"):
        body = api_get(DATA_URL, dict(base, providers=",".join(ids), groupBase=gb,
                                      statFunction=COMPARE_AGG_FUNCS))
        if body is None:
            return False
        rows.update(_compare_rows(body))

    ts = int(hour_start.timestamp() * 1e9)
    points = []
    for (cdnid, country, asn), fields in rows.items():
        p = Point("perfops_cdn_compare") \
            .tag("provider_name", get_provider_name(cdnid)) \
            .tag("country", country if country == "_all" else compare_countries.get(country, f"C_{country}")) \
            .tag("asn", asn) \
            .time(ts, WritePrecision.NS)
        for k, v in fields.items():
            p = p.field(k, v)
        points.append(p)
    for i in range(0, len(points), INFLUX_BATCH_SIZE):
        batch = points[i:i + INFLUX_BATCH_SIZE]
        ok = False
        for attempt in range(1, INFLUX_WRITE_RETRIES + 1):
            try:
                write_api.write(bucket=COMPARE_BUCKET, org=INFLUXDB_ORG, record=batch)
                ok = True
                break
            except Exception as e:
                print(f"[compare] write error ({attempt}/{INFLUX_WRITE_RETRIES}): {e}")
                time.sleep(min(60, 5 * 2 ** (attempt - 1)))
        if not ok:
            return False
    n_asn = sum(1 for k in rows if k[2] != "_all")
    print(f"[compare] {hour_start:%Y-%m-%d %H:00} UTC: {len(points)} points "
          f"({n_asn} country x ASN, {len(ids)} CDNs)")
    return True

def load_compare_checkpoint():
    try:
        with open(COMPARE_CHECKPOINT_FILE) as fh:
            return parse_utc(json.load(fh)["next_hour"])
    except Exception:
        return None

def save_compare_checkpoint(next_hour):
    try:
        with open(COMPARE_CHECKPOINT_FILE, "w") as fh:
            json.dump({"next_hour": next_hour.isoformat()}, fh)
    except Exception as e:
        print(f"[compare] could not save checkpoint: {e}")

def compare_ready():
    if not provider_map:
        return False
    if not compare_countries and not load_compare_countries():
        return False
    return True

def compare_catch_up(write_api):
    """Collect complete hours (older than COMPARE_LAG_MINUTES), at most
    COMPARE_MAX_HOURS_PER_CYCLE per cycle so the raw-log fetch is never held up."""
    if not compare_ready():
        print("[compare] provider / country list not loaded yet; skipping")
        return
    now = datetime.now(timezone.utc)
    last_complete = (now - timedelta(minutes=COMPARE_LAG_MINUTES)).replace(
        minute=0, second=0, microsecond=0)            # hours < last_complete are final
    nxt = load_compare_checkpoint() or last_complete - timedelta(hours=COMPARE_INITIAL_HOURS)
    done = 0
    while nxt < last_complete and done < COMPARE_MAX_HOURS_PER_CYCLE:
        if not compare_hour(write_api, nxt):
            print(f"[compare] hour {nxt} failed; will retry next cycle")
            return
        nxt += timedelta(hours=1)
        save_compare_checkpoint(nxt)
        done += 1

def compare_backfill(write_api, start, end):
    """One-shot: collect every hour in [start, end). Doesn't touch the compare checkpoint."""
    if not compare_ready():
        print("[compare] provider / country list could not be loaded")
        return False
    h = start.replace(minute=0, second=0, microsecond=0)
    while h < end:
        for attempt in range(1, 4):
            if compare_hour(write_api, h):
                break
            time.sleep(30 * attempt)
        else:
            print(f"[compare] giving up at {h}. Resume with --compare-start '{h.isoformat()}' "
                  f"--compare-end '{end.isoformat()}'")
            return False
        h += timedelta(hours=1)
    return True

# --- Main Execution ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PerfOps CDN raw logs -> InfluxDB exporter")
    parser.add_argument("--refetch-start", help="One-shot mode: re-fetch from this UTC time (e.g. 2026-11-03T16:50:00Z), then exit")
    parser.add_argument("--refetch-end", help="One-shot mode: re-fetch up to this UTC time (exclusive)")
    parser.add_argument("--compare-start", help="One-shot mode: collect CDN comparison hours from this UTC time, then exit")
    parser.add_argument("--compare-end", help="One-shot mode: collect CDN comparison hours up to this UTC time (exclusive)")
    args = parser.parse_args()
    if bool(args.refetch_start) != bool(args.refetch_end):
        parser.error("--refetch-start and --refetch-end must be used together")
    if bool(args.compare_start) != bool(args.compare_end):
        parser.error("--compare-start and --compare-end must be used together")

    if not API_KEY:
        print("Error: PERFOPS_API_KEY environment variable not set. Exiting.")
        exit(1)
        
    if not INFLUXDB_TOKEN:
        print("Error: INFLUXDB_TOKEN environment variable not set. Exiting.")
        exit(1)

    # Initialize InfluxDB Client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG,
                            timeout=INFLUX_TIMEOUT_MS)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    if args.compare_start:
        start, end = parse_utc(args.compare_start), parse_utc(args.compare_end)
        ensure_providers(max_attempts=10)
        print(f"[compare] one-shot {start} to {end} -> {COMPARE_BUCKET} (CDNs: {COMPARE_CDNS})")
        try:
            ok = compare_backfill(write_api, start, end)
        finally:
            client.close()
        sys.exit(0 if ok else 1)

    if args.refetch_start:
        start, end = parse_utc(args.refetch_start), parse_utc(args.refetch_end)
        if end <= start:
            parser.error("--refetch-end must be after --refetch-start")
        if not ensure_providers(max_attempts=10):
            print(f"ERROR: provider list / CDN_FILTER '{CDN_FILTER}' could not be loaded; refusing to re-fetch. Exiting.")
            client.close()
            sys.exit(1)
        print(f"Re-fetching {start} to {end} into {INFLUXDB_URL} / bucket: {INFLUXDB_BUCKET} "
              f"(providers: {cdn_filter_ids or 'all'}). Checkpoint is not touched.")
        try:
            ok = refetch(write_api, start, end)
        finally:
            client.close()
        sys.exit(0 if ok else 1)

    ensure_providers()
    print(f"Starting PerfOps to InfluxDB exporter. Writing to {INFLUXDB_URL} / bucket: {INFLUXDB_BUCKET} "
          f"(NETWORK_TAGS={NETWORK_TAGS}, COMPARE={'on' if COMPARE else 'off'})")
    
    try:
        while True:
            cycle_start = time.time()
            # Reload when the list is missing/stale, or the CDN filter hasn't resolved yet
            if (not provider_map or not filter_ready()
                    or time.time() - providers_loaded_at > PROVIDER_REFRESH):
                load_providers()
            catch_up(write_api)
            if COMPARE:
                try:
                    compare_catch_up(write_api)
                except Exception as e:      # never let the comparison stop the raw-log fetch
                    print(f"[compare] unexpected error: {e}")
            elapsed = time.time() - cycle_start
            sleep_for = max(5, SCRAPE_INTERVAL - elapsed)
            print(f"Cycle took {elapsed:.1f}s. Waiting for {sleep_for:.1f} seconds before next fetch.")
            time.sleep(sleep_for)
    finally:
        client.close()