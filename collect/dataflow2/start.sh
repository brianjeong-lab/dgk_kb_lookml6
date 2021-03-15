#!/bin/bash

#for hour in "01" "02" "03" "04" "05" "06" "07" "08" 
for hour in "09" "10" "11" "12" "13" "14" "15" "16" "17"
#for hour in "01" 
do

python3 main.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master_200723" \
    --bq.table "keyword_corona" \
    --input.path "gs://kb-daas-dev-raw-data/rsn/bank_zip" \
    --input.year 2020 \
    --input.month 6 \
    --input.day 2 \
    --input.hour "${hour}" \
    --job_name "load-master-200723-keyword-corona-${hour}" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --max_num_workers 1 \
    --num_workers 1 \
    --number_of_worker_harness_threads 200 \
    --use_public_ips false \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/" &

done

#python -m apache_beam.examples.wordcount \
#  --region "asia-northeast1" \
#  --input gs://dataflow-samples/shakespeare/kinglear.txt \
#  --output gs://kb-daas-dev-raw-data/wordcount/outputs \
#  --runner DataflowRunner \
#  --project $(gcloud config get-value project) \
#  --temp_location gs://kb-daas-dev-raw-data/rsn/temp/
