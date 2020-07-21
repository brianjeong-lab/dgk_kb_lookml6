#!/bin/bash

python3 main.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master" \
    --bq.table "keyword_bank" \
    --input.year 2020 \
    --input.month 6 \
    --input.day 3 \
    --input.hour 12 \
    --job_name "text-analytics-2020-6-3-12" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/" \
    --max_num_workers 1 \
    --number_of_worker_harness_threads 10 \
    --use_public_ips false 

