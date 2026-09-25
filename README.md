# PerfOps Exporter

PerfOps Exporter is a Prometheus exporter that fetches CDN analytics raw logs from the [PerfOps API](https://perfops.net/) and exposes them as Prometheus metrics. 

This exporter extracts critical performance and reliability data, including Time To First Byte (TTFB), cache statuses, and HTTP status codes, enriching them with metadata labels such as provider name, country code, ASN, and ISP name.

## Features

- Exposes **TTFB (Time To First Byte)** as a Prometheus Histogram.
- Exposes **Cache Status** and **HTTP Status** as Prometheus Counters.
- Enriches metrics with labels: `provider_name`, `country_code`, `asn`, `isp_name`.
- Configurable via environment variables.
- Docker-ready with a lightweight multi-stage image.
- Kubernetes deployment ready with Prometheus scraping annotations.

## Prerequisites

- Python 3.9+
- A valid [PerfOps API Key](https://perfops.net/)
- (Optional) Docker for containerized deployment
- (Optional) Kubernetes cluster for deployment

## Metrics Reference

The exporter exposes the following metrics on port `8000` at the root path `/` (and can be scraped anywhere, usually `/metrics` is standard for Prometheus but this exposes at root by default using `prometheus_client`'s `start_http_server`):

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `perfops_cdn_ttfb_seconds` | Histogram | `provider_name`, `country_code`, `asn`, `isp_name` | CDN Time To First Byte from PerfOps in seconds. |
| `perfops_cdn_cache_status_total` | Counter | `provider_name`, `country_code`, `cache_status` | CDN cache status count (e.g., HIT, MISS). |
| `perfops_cdn_http_status_total` | Counter | `provider_name`, `country_code`, `status_code` | CDN HTTP status codes count (e.g., 200, 404, 5xx). |

## Configuration

The exporter requires the following environment variable to authenticate with the PerfOps API:

| Environment Variable | Description | Required |
| :--- | :--- | :--- |
| `PERFOPS_API_KEY` | Your PerfOps API Key. | Yes |

*Note: The exporter currently defaults to a 120-second scrape interval and binds to port 8000. These can be adjusted by editing `SCRAPE_INTERVAL` and `EXPORTER_PORT` in `perfops_exporter.py`.*

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

3. **Set the API Key and run the exporter:**
   ```bash
   export PERFOPS_API_KEY="your_api_key_here"
   python perfops_exporter.py
   ```

4. **Verify metrics:**
   Open a browser or use `curl` to visit `http://localhost:8000/`.

## Running with Docker

1. **Build the Docker image:**
   ```bash
   cd exporter
   docker build -t perfops-exporter:latest .
   ```

2. **Run the container:**
   ```bash
   docker run -d -p 8000:8000 -e PERFOPS_API_KEY="your_api_key_here" perfops-exporter:latest
   ```

## Kubernetes Deployment

A deployment manifest is provided for deploying the exporter to a Kubernetes cluster. It includes Prometheus annotations for automatic metric scraping.

1. **Create a Kubernetes Secret for your API key:**
   ```bash
   kubectl create secret generic perfops-secret --from-literal=PERFOPS_API_KEY='your_api_key_here'
   ```

2. **Update the Docker image reference:**
   Edit the `perfops-exporter-deployment.yaml` file to replace `your-repo/perfops-exporter:latest` with the actual path to your container registry.

3. **Apply the deployment and service:**
   ```bash
   kubectl apply -f perfops-exporter-deployment.yaml
   ```

The exporter will now run as a pod with a service named `perfops-exporter-service` routing traffic to port `8000`. If your Prometheus setup uses Kubernetes annotations, it will automatically discover and scrape the pod.

## Project Structure

```text
perfops-exporter/
├── exporter/
│   ├── Dockerfile             # Multi-stage lightweight Dockerfile
│   ├── perfops_exporter.py    # Main exporter script
│   └── requirements.txt       # Python dependencies
└── perfops-exporter-deployment.yaml # Kubernetes Deployment & Service manifests
```