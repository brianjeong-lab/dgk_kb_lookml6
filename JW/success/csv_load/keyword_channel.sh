#!/bin/bash

bq load \
  --noreplace \
  --source_format=CSV \
  master_200723.keyword_channel \
  gs://kb-daas-dev-raw-data/new_rsn/2020_KB_channel_sample_0723_noheader.csv \
  ./keyword_channel.json
