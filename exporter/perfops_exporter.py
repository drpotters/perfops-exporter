import requests
import json
import os
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
    tmp = GEO_CACHE_FILE + ".tmp"
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

def load_providers():
    global provider_map, providers_loaded_at
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
    except Exception as e:
        print(f"Warning: Could not load CDN providers list: {e}")

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
    tmp = CHECKPOINT_FILE + ".tmp"
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
    cache_status = 'hit' if cache == 1 else 'miss' if cache == 0 else str(cache)
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

    ts_ns = extract_timestamp(row, col_idx, fallback_ns)
    
    # Hash row to create stable sub-second offset to avoid duplicates
    row_str = json.dumps(row, sort_keys=True)
    hash_val = int(hashlib.md5(row_str.encode()).hexdigest(), 16)
    offset_ns = hash_val % 1000000 # up to 1ms offset
    
    point = point.time(ts_ns + offset_ns, WritePrecision.NS)
    return point

def fetch_window(write_api, start_time: datetime, end_time: datetime):
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
                    return False
                time.sleep(2 ** retry_count)
                
        if retry_count >= 3:
            return False

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
            try:
                # Write in batches of INFLUX_BATCH_SIZE (default 5000)
                for i in range(0, len(points), INFLUX_BATCH_SIZE):
                    batch = points[i:i + INFLUX_BATCH_SIZE]
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=batch)
                total_points += len(points)
                pages_fetched += 1
            except ApiException as e:
                print(f"InfluxDB write failed ({e.status}): {e.body}")
                return False
            except Exception as e:
                print(f"An unexpected error occurred during InfluxDB write: {e}")
                return False

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
    return True

def catch_up(write_api):
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

        success = fetch_window(write_api, start, end)
        if not success:
            print(f"Failed to process chunk from {start} to {end}. Stopping catch-up for this cycle.")
            break
        
        save_checkpoint(end)
        start = end
        
        now = datetime.now(timezone.utc) - timedelta(seconds=INGEST_LAG_SECONDS)
        if start < now and BACKFILL_REQUEST_DELAY > 0:
            time.sleep(BACKFILL_REQUEST_DELAY)

# --- Main Execution ---
if __name__ == '__main__':
    if not API_KEY:
        print("Error: PERFOPS_API_KEY environment variable not set. Exiting.")
        exit(1)
        
    if not INFLUXDB_TOKEN:
        print("Error: INFLUXDB_TOKEN environment variable not set. Exiting.")
        exit(1)

    # Initialize InfluxDB Client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    load_providers()
    print(f"Starting PerfOps to InfluxDB exporter. Writing to {INFLUXDB_URL} / bucket: {INFLUXDB_BUCKET}")
    
    try:
        while True:
            cycle_start = time.time()
            catch_up(write_api)
            if not provider_map or time.time() - providers_loaded_at > PROVIDER_REFRESH:
                load_providers()
            elapsed = time.time() - cycle_start
            sleep_for = max(5, SCRAPE_INTERVAL - elapsed)
            print(f"Cycle took {elapsed:.1f}s. Waiting for {sleep_for:.1f} seconds before next fetch.")
            time.sleep(sleep_for)
    finally:
        client.close()