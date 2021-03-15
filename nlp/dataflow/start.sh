#!/bin/bash

python3 main.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master_200722" \
    --bq.table "keyword_bank" \
    --input.year 2020 \
    --input.month 6 \
    --input.day 1 \
    --input.hour 0 \
    --job_name "analytics-nlp-2020-6-1" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/" \
    --max_num_workers 1 \
    --num_workers 1 \
    --number_of_worker_harness_threads 200 \
    --use_public_ips false \
    --requirements_file requirements.txt \
    --service_account_email=838106301395-compute@developer.gserviceaccount.com \
    --setup_file "./setup.py" 

