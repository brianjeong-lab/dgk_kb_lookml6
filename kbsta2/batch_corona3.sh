#!/bin/bash

echo "START `date`"

for ((j=1;j<=20;j++))
do
    #for i in 17 18 19 20 21 22 23 24 25 26 27 28 29 30 16
    #for i in 23 24 25 26 27 28 29 30
    for i in 1
    do 
       echo "START ${j}-${i} : " `date`
 
 python call_to_kb_sta_api_corona.py 19 09 &
 python call_to_kb_sta_api_corona.py 19 10 &
 python call_to_kb_sta_api_corona.py 19 11 &
 python call_to_kb_sta_api_corona.py 19 12 &
 python call_to_kb_sta_api_corona.py 19 13 &
 python call_to_kb_sta_api_corona.py 19 14 &
 python call_to_kb_sta_api_corona.py 19 15 &
 python call_to_kb_sta_api_corona.py 19 16 &
 python call_to_kb_sta_api_corona.py 19 17 &
 python call_to_kb_sta_api_corona.py 19 18 &
 python call_to_kb_sta_api_corona.py 22 08 &
 python call_to_kb_sta_api_corona.py 22 09 &
 python call_to_kb_sta_api_corona.py 22 10 &
 python call_to_kb_sta_api_corona.py 22 11 &
 python call_to_kb_sta_api_corona.py 22 12 &
 python call_to_kb_sta_api_corona.py 22 13 &
 python call_to_kb_sta_api_corona.py 22 14 &
 python call_to_kb_sta_api_corona.py 22 16 &
 python call_to_kb_sta_api_corona.py 22 17 &
 python call_to_kb_sta_api_corona.py 22 18 &
 python call_to_kb_sta_api_corona.py 22 19 &
 python call_to_kb_sta_api_corona.py 22 15 

 python call_to_kb_sta_api_corona.py 16 08 &
 python call_to_kb_sta_api_corona.py 16 09 &
 python call_to_kb_sta_api_corona.py 16 10 &
 python call_to_kb_sta_api_corona.py 16 11 &
 python call_to_kb_sta_api_corona.py 16 12 &
 python call_to_kb_sta_api_corona.py 16 13 &
 python call_to_kb_sta_api_corona.py 16 14 &
 python call_to_kb_sta_api_corona.py 16 15 &
 python call_to_kb_sta_api_corona.py 16 16 &
 python call_to_kb_sta_api_corona.py 16 17 &
 python call_to_kb_sta_api_corona.py 16 18 &
 python call_to_kb_sta_api_corona.py 17 08 &
 python call_to_kb_sta_api_corona.py 17 09 &
 python call_to_kb_sta_api_corona.py 17 10 &
 python call_to_kb_sta_api_corona.py 17 12 &
 python call_to_kb_sta_api_corona.py 17 13 &
 python call_to_kb_sta_api_corona.py 17 14 &
 python call_to_kb_sta_api_corona.py 17 15 &
 python call_to_kb_sta_api_corona.py 17 16 &
 python call_to_kb_sta_api_corona.py 17 17 &
 python call_to_kb_sta_api_corona.py 17 18 &
 python call_to_kb_sta_api_corona.py 17 19 & 
 python call_to_kb_sta_api_corona.py 18 09 &
 python call_to_kb_sta_api_corona.py 18 10 &
 python call_to_kb_sta_api_corona.py 18 11 &
 python call_to_kb_sta_api_corona.py 18 12 &
 python call_to_kb_sta_api_corona.py 18 13 &
 python call_to_kb_sta_api_corona.py 18 14 &
 python call_to_kb_sta_api_corona.py 18 15 &
 python call_to_kb_sta_api_corona.py 18 16 &
 python call_to_kb_sta_api_corona.py 18 17 &
 python call_to_kb_sta_api_corona.py 18 18 &
 python call_to_kb_sta_api_corona.py 17 11  

       echo "END ${j}-${i} : " `date`
    done
done
echo "END `date`"
