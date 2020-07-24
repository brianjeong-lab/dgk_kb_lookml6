#!/bin/bash

bq load \
  --noreplace \
  --source_format=CSV \
  master_200723.keyword_channel_addtion \
  gs://kb-daas-dev-raw-data/new_rsn/KBSTAR_3rd_0715_0721_noheader.csv \
  ./kbstar_3rd.json
