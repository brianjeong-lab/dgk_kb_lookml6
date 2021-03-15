#!/bin/bash

echo "START `date`"

for ((j=1;j<=1;j++))
do
    for i in 0
    do 
       echo "START ${j}-${i} : " `date`
       #python call_to_kb_sta_api.py "04" &
       python call_to_kb_sta_api.py "10" &
       #python call_to_kb_sta_api.py "11" &
       #python call_to_kb_sta_api.py "15" &
       python call_to_kb_sta_api.py "16" &
       echo "END ${j}-${i} : " `date`
    done
done
echo "END `date`"
