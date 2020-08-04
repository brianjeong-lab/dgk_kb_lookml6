#!/bin/bash

echo "START `date`"

for ((j=1;j<=10;j++))
do
    #for i in 17 18 19 20 21 22 23 24 25 26 27 28 29 30 16
    for i in 23 24 25 26 27 28 29 30
    do 
       echo "START ${j}-${i} : " `date`
       python call_to_kb_sta_api_corona.py "${i}" "00" &
       python call_to_kb_sta_api_corona.py "${i}" "01" &
       python call_to_kb_sta_api_corona.py "${i}" "02" &
       python call_to_kb_sta_api_corona.py "${i}" "03" &
       python call_to_kb_sta_api_corona.py "${i}" "04" &
       python call_to_kb_sta_api_corona.py "${i}" "05" &
       python call_to_kb_sta_api_corona.py "${i}" "06" &
       python call_to_kb_sta_api_corona.py "${i}" "07" &
       python call_to_kb_sta_api_corona.py "${i}" "08" &
       python call_to_kb_sta_api_corona.py "${i}" "09" &
       #python call_to_kb_sta_api_corona.py "${i}" "10" &
       python call_to_kb_sta_api_corona.py "${i}" "11" &
       python call_to_kb_sta_api_corona.py "${i}" "12" &
       python call_to_kb_sta_api_corona.py "${i}" "13" &
       python call_to_kb_sta_api_corona.py "${i}" "14" &
       python call_to_kb_sta_api_corona.py "${i}" "15" &
       python call_to_kb_sta_api_corona.py "${i}" "16" &
       python call_to_kb_sta_api_corona.py "${i}" "17" &
       python call_to_kb_sta_api_corona.py "${i}" "18" &
       python call_to_kb_sta_api_corona.py "${i}" "19" &
       python call_to_kb_sta_api_corona.py "${i}" "20" &
       python call_to_kb_sta_api_corona.py "${i}" "21" &
       python call_to_kb_sta_api_corona.py "${i}" "22" &
       python call_to_kb_sta_api_corona.py "${i}" "23" &
       python call_to_kb_sta_api_corona.py "${i}" "10" 
       echo "END ${j}-${i} : " `date`
    done
done
echo "END `date`"
