import requests
import json
import os
import time
from prometheus_client import start_http_server, Histogram, Counter

# --- Configuration ---
API_KEY = os.environ.get("PERFOPS_API_KEY")
BASE_URL = "https://api.perfops.net/v2/analytics/cdn/rawlogs"
HEADERS = {
    "Authorization": f"{API_KEY}",
    "Content-Type": "application/json"
}
SCRAPE_INTERVAL = 120
EXPORTER_PORT = 8000

# --- Prometheus Metric Definitions ---
TTFB_HISTOGRAM = Histogram(
    'perfops_cdn_ttfb_seconds',
    'CDN Time To First Byte from PerfOps',
    ['provider_name', 'country_code', 'asn', 'isp_name']
)
CACHE_STATUS_COUNTER = Counter(
    'perfops_cdn_cache_status_total',
    'CDN cache status from PerfOps',
    ['provider_name', 'country_code', 'cache_status']
)
HTTP_STATUS_COUNTER = Counter(
    'perfops_cdn_http_status_total',
    'CDN HTTP status codes from PerfOps',
    ['provider_name', 'country_code', 'status_code']
)

# --- Data Fetching and Processing ---
def process_logs():
    print("Fetching latest logs from PerfOps API...")
    try:
        params = {"page": 1, "limit": 10000}
        response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        logs = response.json()

        if not logs:
            print("No new logs found.")
            return

        print(f"Processing {len(logs)} log entries...")
        for log in logs:
            performance = log.get('performance', {})
            ttfb = performance.get('ttfb')
            
            labels = {
                'provider_name': log.get('provider_name', 'unknown'),
                'country_code': log.get('country_code', 'unknown'),
                'asn': str(log.get('asn', 'unknown')),
                'isp_name': log.get('isp_name', 'unknown')
            }

            if ttfb is not None and ttfb > 0:
                TTFB_HISTOGRAM.labels(**labels).observe(ttfb / 1000.0)

            cache_status = performance.get('cache_status', 'unknown')
            CACHE_STATUS_COUNTER.labels(
                provider_name=labels['provider_name'],
                country_code=labels['country_code'],
                cache_status=cache_status
            ).inc()

            http_status = str(performance.get('http_status', 'unknown'))
            HTTP_STATUS_COUNTER.labels(
                provider_name=labels['provider_name'],
                country_code=labels['country_code'],
                status_code=http_status
            ).inc()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from PerfOps API: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Main Execution ---
if __name__ == '__main__':
    if not API_KEY:
        print("Error: PERFOPS_API_KEY environment variable not set. Exiting.")
    else:
        start_http_server(EXPORTER_PORT)
        print(f"Prometheus exporter started on port {EXPORTER_PORT}")
        while True:
            process_logs()
            print(f"Waiting for {SCRAPE_INTERVAL} seconds before next fetch.")
            time.sleep(SCRAPE_INTERVAL)

