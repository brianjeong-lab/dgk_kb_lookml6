#!/bin/bash

for ((j=1;j<=100;j++))
do
    for i in 0 1 2 
    do 
       echo "START ${j}-${i} : " `date`
       #python call_to_nlp_api2.py "${i}1" &
       #python call_to_nlp_api2.py "${i}2" &
       #python call_to_nlp_api2.py "${i}3" &
       #python call_to_nlp_api2.py "${i}4" &
       #python call_to_nlp_api2.py "${i}5" &
       #python call_to_nlp_api2.py "${i}6" &
       #python call_to_nlp_api2.py "${i}7" &
       #python call_to_nlp_api2.py "${i}8" &
       #python call_to_nlp_api2.py "${i}9" 
       echo "END ${j}-${i} : " `date`
    done
done

echo "END `date`"
