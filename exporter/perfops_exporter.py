import requests
import json
import os
import time
from datetime import datetime, timedelta, timezone
from prometheus_client import start_http_server, Histogram, Counter

# --- Configuration ---
API_KEY = os.environ.get("PERFOPS_API_KEY")
BASE_URL = "https://api.perfops.net/analytics/cdn/raw-logs"
PROVIDERS_URL = "https://api.perfops.net/analytics/cdn/provider"
HEADERS = {
    "Authorization": f"{API_KEY}",
    "Content-Type": "application/json"
}
SCRAPE_INTERVAL = 120
EXPORTER_PORT = 8000

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

# --- Prometheus Metric Definitions ---
# Base labels to include on almost everything
LABELS = ['provider_name', 'continent', 'country', 'city']
EXTENDED_LABELS = LABELS + ['platform', 'httpVersion']

# Histograms (converting ms to seconds)
# Using standard prometheus buckets (default is usually up to 10s)
TTFB_HISTOGRAM = Histogram(
    'perfops_cdn_ttfb_seconds',
    'CDN Time To First Byte from PerfOps',
    EXTENDED_LABELS
)
DNS_HISTOGRAM = Histogram(
    'perfops_cdn_dns_lookup_seconds',
    'CDN DNS Lookup Time from PerfOps',
    LABELS
)
TCP_HISTOGRAM = Histogram(
    'perfops_cdn_tcp_connect_seconds',
    'CDN TCP Connect Time from PerfOps',
    LABELS
)
SSL_HISTOGRAM = Histogram(
    'perfops_cdn_ssl_handshake_seconds',
    'CDN SSL Handshake Time from PerfOps',
    LABELS
)
TRANSFER_HISTOGRAM = Histogram(
    'perfops_cdn_transfer_seconds',
    'CDN Data Transfer Time from PerfOps',
    LABELS
)
TOTAL_LATENCY_HISTOGRAM = Histogram(
    'perfops_cdn_total_latency_seconds',
    'CDN Total Latency from PerfOps',
    EXTENDED_LABELS
)

# Counters
CACHE_STATUS_COUNTER = Counter(
    'perfops_cdn_cache_status_total',
    'CDN cache status from PerfOps',
    ['provider_name', 'continent', 'country', 'cache_status']
)
HTTP_STATUS_COUNTER = Counter(
    'perfops_cdn_http_status_total',
    'CDN HTTP status codes from PerfOps',
    ['provider_name', 'continent', 'country', 'status_code']
)
FAILURE_COUNTER = Counter(
    'perfops_cdn_failures_total',
    'CDN failure reasons from PerfOps',
    ['provider_name', 'continent', 'country', 'failure_reason']
)

# --- Data Fetching and Processing ---
def process_logs():
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
            
            base_labels = {
                'provider_name': provider_name,
                'continent': continent,
                'country': country,
                'city': city
            }
            ext_labels = dict(base_labels)
            ext_labels.update({
                'platform': platform,
                'httpVersion': http_version
            })

            # Record Histograms (convert ms to seconds)
            ttfb = get_val('ttfb')
            if isinstance(ttfb, (int, float)) and ttfb > 0:
                TTFB_HISTOGRAM.labels(**ext_labels).observe(ttfb / 1000.0)
                
            dns = get_val('dnsLookupTimeMs')
            if isinstance(dns, (int, float)) and dns >= 0:
                DNS_HISTOGRAM.labels(**base_labels).observe(dns / 1000.0)
                
            tcp = get_val('tcpTimeMs')
            if isinstance(tcp, (int, float)) and tcp >= 0:
                TCP_HISTOGRAM.labels(**base_labels).observe(tcp / 1000.0)
                
            ssl = get_val('sslTimeMs')
            if isinstance(ssl, (int, float)) and ssl >= 0:
                SSL_HISTOGRAM.labels(**base_labels).observe(ssl / 1000.0)
                
            transfer = get_val('transferTime')
            if isinstance(transfer, (int, float)) and transfer >= 0:
                TRANSFER_HISTOGRAM.labels(**base_labels).observe(transfer / 1000.0)
                
            total_ms = get_val('ms')
            if isinstance(total_ms, (int, float)) and total_ms > 0:
                TOTAL_LATENCY_HISTOGRAM.labels(**ext_labels).observe(total_ms / 1000.0)

            # Record Counters
            # Cache status (usually 1 for HIT, 0 for MISS, etc.)
            cache = get_val('cache')
            cache_status = 'hit' if cache == 1 else 'miss' if cache == 0 else str(cache)
            CACHE_STATUS_COUNTER.labels(
                provider_name=provider_name,
                continent=continent,
                country=country,
                cache_status=cache_status
            ).inc()

            status_code = str(get_val('statusCode', 'unknown'))
            HTTP_STATUS_COUNTER.labels(
                provider_name=provider_name,
                continent=continent,
                country=country,
                status_code=status_code
            ).inc()
            
            failure = str(get_val('failureReason', 'NONE'))
            if failure and failure != 'NONE':
                FAILURE_COUNTER.labels(
                    provider_name=provider_name,
                    continent=continent,
                    country=country,
                    failure_reason=failure
                ).inc()

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
    else:
        load_providers()
        start_http_server(EXPORTER_PORT)
        print(f"Prometheus exporter started on port {EXPORTER_PORT}")
        while True:
            process_logs()
            print(f"Waiting for {SCRAPE_INTERVAL} seconds before next fetch.")
            time.sleep(SCRAPE_INTERVAL)

