#!/bin/bash

# Variables
PROJECT_ID="shabubsinc"
REGION="europe-west1"
SERVICE_NAME="fear-greed-ingestion-service"
SCHEDULER_JOB_NAME="fear-greed-scheduler-job"
SCHEDULE="0 0 * * *"  # Once daily

# Deploy the Cloud Run service
gcloud run deploy $SERVICE_NAME \
  --source . \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated

# Fetch the new Cloud Run URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format="get(status.url)")

echo "Fetched Cloud Run URL: $SERVICE_URL"

# Update Cloud Scheduler with the new URL and root endpoint "/"
gcloud scheduler jobs update http $SCHEDULER_JOB_NAME \
  --location $REGION \
  --schedule "$SCHEDULE" \
  --uri "$SERVICE_URL/" \
  --oidc-service-account-email "cloud-run-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --http-method POST \
  --message-body '{"trigger": "start_ingestion"}'

echo "Cloud Scheduler updated with new URL"
