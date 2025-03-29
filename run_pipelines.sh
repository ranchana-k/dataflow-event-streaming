#!/bin/bash

set -e

python3 -m pip install -r requirements.txt

python3 main.py \
  --aggregation_window_sec=${_AGG_WINDOW:-60} \
  --alert_window_sec=${_ALERT_WINDOW:-1} \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=${YOUR_REGION} \
  --temp_location=gs://${YOUR_BUCKET}/temp \
  --staging_location=gs://${YOUR_BUCKET}/staging \
  --job_name=streaming-purchase-alerts-$(date +%Y%m%d%H%M%S)
