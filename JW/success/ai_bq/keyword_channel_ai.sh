#!/bin/bash

# bq.table 테이블명 변경, 날짜: 빅쿼리 sql where조건 날짜, job_name : 데이터플로우상에 동작 잡 이름, 실행결과는 자동으로 "테이블명_result"로 들어감(테이블 먼저 존재해야함)

python3 keyword_channel_ai.py \
    --bq.project "$(gcloud config get-value project)" \
    --bq.dataset "master_200723" \
    --bq.table "keyword_channel_addtion" \
    --input.year 2020 \
    --input.month 7 \
    --input.day 8 \
    --input.hour 0 \
    --job_name "ai-bq-channel" \
    --project "$(gcloud config get-value project)" \
    --runner DataflowRunner \
    --num_workers 10 \
    --max_num_workers 10 \
    --region "asia-northeast1" \
    --temp_location "gs://kb-daas-dev-raw-data/rsn/temp/"

# 18127
