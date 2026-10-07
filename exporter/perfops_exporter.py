import requests
import json
import os
import time
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
API_KEY = os.environ.get("PERFOPS_API_KEY")
BASE_URL = "https://api.perfops.net/analytics/cdn/raw-logs"
PROVIDERS_URL = "https://api.perfops.net/analytics/cdn/provider"
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

# Fetch providers at startup
provider_map = {}

def load_providers():
    global provider_map
    print("Loading CDN providers...")
    try:
        response = requests.get(PROVIDERS_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        providers = response.json()
        for p in providers:
            provider_map[p.get('id')] = p.get('name', f"Provider_{p.get('id')}")
        print(f"Loaded {len(provider_map)} CDN providers.")
    except Exception as e:
        print(f"Warning: Could not load CDN providers list: {e}")

def get_provider_name(cdnid):
    if cdnid in provider_map:
        return provider_map[cdnid]
    return f"CDN_{cdnid}"

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
        for row in values:
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

            # Set write time
            # The raw logs don't provide an exact timestamp per row, so we use the fetch time.
            point = point.time(now, WritePrecision.NS)

            points.append(point)

        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            print(f"Successfully wrote {len(points)} points to InfluxDB.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from PerfOps API: {e}")
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
            print(f"Waiting for {SCRAPE_INTERVAL} seconds before next fetch.")
            time.sleep(SCRAPE_INTERVAL)
    finally:
        client.close()

