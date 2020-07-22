#!/bin/bash

DAY=$(seq 16 24)
HOUR=$(seq 0 23)

for day in $DAY
do
  echo "Running load csv day "$day

  for hour in $HOUR
  do
    echo "Running load csv time "$hour

    python3 gcs_bq_0722.py \
        --bq.project "$(gcloud config get-value project)" \
        --bq.dataset "master_200722" \
        --bq.table "keyword_corona" \
        --input.gcs_path "gs://kb-daas-dev-raw-data/rsn/corona/6/$day/$hour.csv" \
        --job_name "load-rawdata-2020-6-$day-$hour-corona" \
        --project "$(gcloud config get-value project)" \
        --runner DataflowRunner \
        --region "asia-northeast1" \
        --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/"
  done
done
# corona data load
