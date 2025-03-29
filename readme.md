# Streaming Dataflow Pipeline with Alerting

This repository contains a Python Apache Beam pipeline that:
- Reads purchase events from Pub/Sub
- Aggregates purchases in a configurable window and writes results to BigQuery
- Detects spikes in purchases (>= 100 in short windows) and publishes alerts to a separate Pub/Sub topic

## Components
- `main.py`: Beam pipeline code
- `requirements.txt`: Dependencies
- `cloudbuild.yaml`: CI/CD config for deploying via Cloud Build

## Deployment
1. Replace the placeholders in `cloudbuild.yaml`
2. Push to GitHub and connect Cloud Build trigger
3. Monitor jobs in the Dataflow console

## Configuration
You can set custom window durations:
- `--aggregation_window_sec=60` for smoother BQ summaries
- `--alert_window_sec=1` for spike detection

## IAM Roles Required
Ensure your Dataflow service account (e.g., `<project-number>-compute@developer.gserviceaccount.com`) has these roles:
- `roles/dataflow.worker`: to run Dataflow jobs
- `roles/pubsub.subscriber`: to read from Pub/Sub
- `roles/pubsub.publisher`: to write alerts to Pub/Sub
- `roles/bigquery.dataEditor`: to write aggregated results to BigQuery

## Alert Format (Pub/Sub Message)
```json
{
  "region": "th",
  "device": "mobile",
  "event_count": 112
}
