import requests
import json
import os
import time
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
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

# --- Geocoding configuration ---
GEO_CACHE_FILE = os.environ.get("GEO_CACHE_FILE", "geo_cache.json")
# Nominatim requires an identifying user agent (put your own contact address here)
GEOCODER_USER_AGENT = os.environ.get("GEOCODER_USER_AGENT", "perfops-exporter (ops@example.com)")

GEO_OVERRIDES = {
    "Ashburn|United States":  [39.0438, -77.4874],
    "Pasco|United States":    [46.2396, -119.1006],
    "Suffolk|United States":  [36.7282, -76.5836],
    "Alameda|United States":  [37.7652, -122.2416],
    "Dnipro|Ukraine":         [48.4647, 35.0462],
    "Ain Beida|Algeria":      [35.7964, 7.3928],
}

_geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT, timeout=10)
# Nominatim's usage policy allows at most 1 request per second
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
    with open(tmp, "w") as f:
        json.dump(geo_cache, f)
    os.replace(tmp, GEO_CACHE_FILE)   # atomic write so the cache never gets corrupted


def get_city_coords(city, country):
    """Return [lat, lon] for a city, or None. Results (including misses) are cached on disk."""
    if not city or city in ("unknown", "None", "?", ""):
        return None

    key = f"{city}|{country}"
    if key in GEO_OVERRIDES:
        return GEO_OVERRIDES[key]

    if key in geo_cache:
        return geo_cache[key]

    kwargs = {}
    if len(country) == 2 and country.isalpha():        # ISO-2 code, e.g. "US", "DE"
        kwargs["country_codes"] = country.lower()
        query = city
    elif country and country != "unknown":             # full country name
        query = f"{city}, {country}"
    else:
        query = city

    try:
        loc = _geocode(query, **kwargs)
    except Exception as e:
        print(f"Geocoding failed for '{query}': {e}")   # temporary error: don't cache, retry next time
        return None

    coords = [round(loc.latitude, 4), round(loc.longitude, 4)] if loc else None
    if coords is None:
        print(f"No geocoding result for '{query}' ({country})")
    geo_cache[key] = coords
    save_geo_cache()
    return coords

# Fetch providers at startup
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

# --- Data Fetching and Processing ---
def process_logs(write_api):
    now = datetime.now(timezone.utc)
    # Query the last SCRAPE_INTERVAL seconds, plus a small buffer
    start_time = now - timedelta(seconds=SCRAPE_INTERVAL + 10)
    
    date_from = start_time.strftime('%Y-%m-%d %H:%M:%S')
    date_to = now.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"Fetching logs from {date_from} to {date_to}...")
    try:
        params = {
            "dateTimeFrom": date_from,
            "dateTimeTo": date_to,
            "page": 1,
            "pagelimit": 200 # Max allowed per page
        }
        response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
        
        # Help diagnose if we hit another 404/403
        if response.status_code != 200:
            print(f"API Error {response.status_code}: {response.text}")
        response.raise_for_status()
        
        body = response.json()
        
        # Check if the result array exists and has data
        result_array = body.get('result', [])
        if not result_array or 'data' not in result_array[0] or not result_array[0]['data']:
            print("No new logs found.")
            return
            
        data_block = result_array[0]['data'][0]
        columns = data_block.get('columns', [])
        values = data_block.get('values', [])

        if not columns or not values:
            print("No new logs found.")
            return

        print(f"Processing {len(values)} log entries...")
        
        # Map column names to their index
        col_idx = {name: idx for idx, name in enumerate(columns)}
        
        points = []
        base_ns = int(now.timestamp() * 1e9)    # NEW: base timestamp in nanoseconds
        for i, row in enumerate(values):        # CHANGED: enumerate so each row gets a unique time
            # Helper to extract value safely
            def get_val(col_name, default=None):
                if col_name in col_idx:
                    idx = col_idx[col_name]
                    if idx < len(row):
                        return row[idx]
                return default

            cdnid = get_val('cdnid')
            if not cdnid:
                continue
                
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

            # Record Fields (keeping them as ms to preserve precision)
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

            # NEW: coordinates – use the API's own columns if present, otherwise geocode
            api_lat = get_val('latitude', get_val('lat'))
            api_lon = get_val('longitude', get_val('lon'))
            if isinstance(api_lat, (int, float)) and isinstance(api_lon, (int, float)):
                coords = [float(api_lat), float(api_lon)]
            else:
                coords = get_city_coords(city, country)

            if coords:
                point = point.field("lat", float(coords[0])).field("lon", float(coords[1]))

            # CHANGED: unique timestamp per row (replaces point.time(now, ...))
            point = point.time(base_ns + i, WritePrecision.NS)
            points.append(point)

        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            print(f"Successfully wrote {len(points)} points to InfluxDB.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from PerfOps API: {e}")
    except ApiException as e:
        print(f"InfluxDB write failed ({e.status}): {e.body}")
    except Exception as e:
        import traceback
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()

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
            process_logs(write_api)
            if not provider_map or time.time() - providers_loaded_at > PROVIDER_REFRESH:
                load_providers()
            print(f"Waiting for {SCRAPE_INTERVAL} seconds before next fetch.")
            time.sleep(SCRAPE_INTERVAL)
    finally:
        client.close()

