#!/bin/bash

set -ex

python3 -m pip install -r requirements.txt
echo "🚀 Launching Dataflow job..."

python3 main.py \
  --input_topic=projects/$PROJECT_ID/topics/streaming-events \
  --output_table=$PROJECT_ID:dataflow_test.purchase_summary \
  --alert_topic=projects/$PROJECT_ID/topics/high-volumn-alerts \
  --aggregation_window_sec=${AGG_WINDOW:-60} \
  --alert_window_sec=${ALERT_WINDOW:-1} \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=${REGION} \
  --temp_location=gs://${BUCKET}/temp \
  --staging_location=gs://${BUCKET}/staging \
  --job_name=streaming-purchase-alerts-$(date +%Y%m%d%H%M%S) \
  --service-account-email=${SERVICE_ACCOUNT_EMAIL}
echo "🚀 Done Submitting Dataflow job..."
