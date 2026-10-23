# PerfOps Exporter

PerfOps Exporter fetches CDN analytics raw logs from the [PerfOps API](https://perfops.net/) and pushes them into an **InfluxDB** time-series database. 

This exporter extracts critical performance and reliability data, including Time To First Byte (TTFB), cache statuses, and HTTP status codes, enriching them with metadata tags such as provider name, country code, ASN, city, and platform. 

*Note: This script was previously a Prometheus Exporter but has been rewritten to push data directly to InfluxDB to avoid unbounded memory usage by the script.*

## Features

- Pushes **TTFB (Time To First Byte)** and other latency metrics to InfluxDB as fields (in ms).
- Pushes **Cache Status** and **HTTP Status** to InfluxDB as tags.
- Enriches metrics with tags: `provider_name`, `continent`, `country`, `city`, `platform`, `httpVersion`.
- Configurable via environment variables.
- Docker-ready with a lightweight multi-stage image.
- Kubernetes deployment ready.

## Prerequisites

- Python 3.9+
- A valid [PerfOps API Key](https://perfops.net/)
- An accessible [InfluxDB v2](https://www.influxdata.com/) server or InfluxDB Cloud instance
- (Optional) Docker for containerized deployment
- (Optional) Kubernetes cluster for deployment

## InfluxDB Schema

Data is written to InfluxDB under the measurement `perfops_cdn_logs`.

| Data Type | Name | Description |
| :--- | :--- | :--- |
| **Measurement** | `perfops_cdn_logs` | The base measurement name. |
| **Tags** | `provider_name`, `continent`, `country`, `city`, `platform`, `httpVersion`, `cache_status`, `status_code`, `failure_reason` | Indexed metadata to group and filter queries. |
| **Fields** | `ttfb_ms`, `dns_lookup_ms`, `tcp_connect_ms`, `ssl_handshake_ms`, `transfer_ms`, `total_latency_ms` | Float values representing milliseconds. |

## Configuration

The exporter requires the following environment variables to authenticate with the PerfOps API and InfluxDB:

| Environment Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `PERFOPS_API_KEY` | Your PerfOps API Key. | **Yes** | - |
| `INFLUXDB_TOKEN` | Your InfluxDB API Token. | **Yes** | - |
| `INFLUXDB_URL` | The URL of your InfluxDB instance. | No | `http://localhost:8086` |
| `INFLUXDB_ORG` | Your InfluxDB Organization. | No | `perfops` |
| `INFLUXDB_BUCKET` | Your InfluxDB Bucket to write into. | No | `perfops-cdn` |
| `SCRAPE_INTERVAL` | Interval between API fetches in seconds. | No | `120` |

### Backfill & Resiliency Configuration

The exporter maintains a checkpoint file to ensure it can resume scraping from where it left off after a crash or maintenance downtime, up to the 48-hour API retention limit.

| Environment Variable | Description | Default |
| :--- | :--- | :--- |
| `CHECKPOINT_FILE` | Where progress is saved to resume after a restart. | `/data/checkpoint.json` |
| `MAX_BACKFILL_HOURS` | Longest gap it will go back and fill (PerfOps retention limit is 48h). | `48` |
| `RETENTION_SAFETY_MINUTES` | Stays clear of logs about to expire at the 48h edge. | `5` |
| `BACKFILL_CHUNK_MINUTES` | Size of each catch-up window requested from the API. | `10` |
| `OVERLAP_SECONDS` | Re-reads a little of the last window to catch late-arriving logs. | `60` |
| `INGEST_LAG_SECONDS` | Optional delay before reading recent logs to wait for pipeline flush. | `0` |
| `INITIAL_BACKFILL_HOURS` | Used for the very first run when no checkpoint exists. Defaults to 0 (today's behavior). Set to 48 for a full initial pull. | `0` |
| `BACKFILL_REQUEST_DELAY` | Pause in seconds between catch-up requests. | `0.5` |
| `MAX_PAGES_PER_WINDOW` | Safety limit on how many pages to fetch per time chunk. | `500` |

> **Note on Backfill Behavior:** 
> When the exporter restarts, it will fetch historical logs in smaller chunks (defined by `BACKFILL_CHUNK_MINUTES`) oldest-first until it reaches the present. If the outage exceeds `MAX_BACKFILL_HOURS` (48 hours), you will see a warning in the logs indicating an unrecoverable gap. Data replayed this way will be written to InfluxDB using the actual event time in the log instead of the scrape time, allowing accurate historical graph plotting.

## Running Locally

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd perfops-exporter/exporter
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set the required environment variables and run the exporter:**
   ```bash
   export PERFOPS_API_KEY="your_perfops_api_key_here"
   export INFLUXDB_TOKEN="your_influxdb_token_here"
   export INFLUXDB_URL="http://localhost:8086"
   python perfops_exporter.py
   ```

## Running with Docker

1. **Build the Docker image:**
   ```bash
   cd exporter
   docker build -t perfops-exporter:latest .
   ```

2. **Run the container:**
   ```bash
   docker run -d \
     -e PERFOPS_API_KEY="your_api_key_here" \
     -e INFLUXDB_TOKEN="your_influxdb_token_here" \
     -e INFLUXDB_URL="http://your-influx-server:8086" \
     perfops-exporter:latest
   ```

## Kubernetes Deployment

A deployment manifest is provided for deploying the exporter to a Kubernetes cluster. 

1. **Create a Kubernetes Secret for your API keys:**
   ```bash
   kubectl create secret generic perfops-secret \
     --from-literal=PERFOPS_API_KEY='your_api_key_here' \
     --from-literal=INFLUXDB_TOKEN='your_influxdb_token_here'
   ```

2. **Update the Deployment Configuration:**
   Edit the `perfops-exporter-deployment.yaml` file to replace `your-repo/perfops-exporter:latest` with the actual path to your container registry. Update the `INFLUXDB_URL` pointing to your InfluxDB service.

3. **Apply the deployment:**
   ```bash
   kubectl apply -f perfops-exporter-deployment.yaml
   ```

## Project Structure

```text
perfops-exporter/
├── exporter/
│   ├── Dockerfile             # Multi-stage lightweight Dockerfile
│   ├── perfops_exporter.py    # Main exporter script
│   └── requirements.txt       # Python dependencies
└── perfops-exporter-deployment.yaml # Kubernetes Deployment manifest
```
