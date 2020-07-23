#!/bin/bash

echo "START `date`"

for ((j=1;j<=100;j++))
do
    for i in 0 1 2 
    do 
       echo "START ${j}-${i} : " `date`
       python call_to_kb_sta_api.py "${i}1" &
       python call_to_kb_sta_api.py "${i}2" &
       python call_to_kb_sta_api.py "${i}3" &
       python call_to_kb_sta_api.py "${i}4" &
       python call_to_kb_sta_api.py "${i}5" &
       python call_to_kb_sta_api.py "${i}6" &
       python call_to_kb_sta_api.py "${i}7" &
       python call_to_kb_sta_api.py "${i}8" &
       python call_to_kb_sta_api.py "${i}9" &
       python call_to_kb_sta_api.py "$((i+1))0"
       echo "END ${j}-${i} : " `date`
    done
done
echo "END `date`"