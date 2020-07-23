# Lading rawdata from GCS to Bigquery
### Install
```
pip install -r requirements.txt
```

### Usage
#### Run locally using DirectRunner
```
python main.py \
--input data/<path-to-apache-log-file>.log \
--output filtered-data.txt
```

#### Run on dataflow
```
python3 main.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master_200723" \
    --bq.table "keyword_bank" \
    --input.path "gs://kb-daas-dev-raw-data/rsn/bank_zip" \
    --input.year 2020 \
    --input.month 6 \
    --input.day 2 \
    --input.hour 0 \
    --job_name "load-master_200723_keyword_bank" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --max_num_workers 1 \
    --num_workers 1 \
    --number_of_worker_harness_threads 200 \
    --use_public_ips false \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/"
```

For detailed instruction, read this article - https://medium.com/@rajeshhegde/data-pipeline-using-apache-beam-python-sdk-on-dataflow-6bb8550bf366