#!/bin/bash

python3 main.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master" \
    --bq.table "keyword_bank" \
    --input.path "gs://kb-daas-dev-raw-data/rsn/bank_zip" \
    --input.year 2020 \
    --input.month 6 \
    --input.day 2 \
    --input.hour 0 \
    --job_name "load-rawdata-2020-6-2-0" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/"


#python -m apache_beam.examples.wordcount \
#  --region "asia-northeast1" \
#  --input gs://dataflow-samples/shakespeare/kinglear.txt \
#  --output gs://kb-daas-dev-raw-data/wordcount/outputs \
#  --runner DataflowRunner \
#  --project $(gcloud config get-value project) \
#  --temp_location gs://kb-daas-dev-raw-data/rsn/temp/
