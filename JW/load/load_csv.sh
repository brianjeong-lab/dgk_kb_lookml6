#!/bin/bash

NAME='corona'
MONTH=6
DAY=$(seq 16 30)
TIME=$(seq 0 23)

for d in $DAY
do
  echo "Running load csv day "$d

  for t in $TIME
  do
      echo "Running load csv time "$t

      bq load \
          --noreplace \
          --source_format=CSV \
          raw_data.rsn_corona \
          gs://kb-daas-dev-raw-data/rsn/$NAME/$MONTH/$d/$t.csv \
          ./rsn_data.json
      # some instructions
  done

done
