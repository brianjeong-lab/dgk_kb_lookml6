#!/bin/bash

KEYWORD=bank
FILE_NUMBER=$(seq 1 3)

for num in $FILE_NUMBER
do
  echo "Running load csv num: "$num

  python3 gcs_bq_0722.py \
      --bq.project "$(gcloud config get-value project)" \
      --bq.dataset "master_200722" \
      --bq.table "keyword_"$KEYWORD \
      --input.gcs_path "gs://my_test_bk_0630/bank_data_0630/KBSTAR1_0616_0630_0$num.csv.gz" \
      --job_name "load-rawdata-0$num-"$KEYWORD \
      --project "$(gcloud config get-value project)" \
      --runner DataflowRunner \
      --region "asia-northeast1" \
      --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/"
done
