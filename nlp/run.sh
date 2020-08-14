echo "START `date`"
for j in 1 2 3 4 5 6 7 8 9 10
do
    for i in 1 2 3 4 5 6 7 8 9 10
    do 
       echo "START ${j}-${i} : " `date`
       python call_to_nlp_api.py
       echo "END${j}-${i} : " `date`
    done
done
echo "END `date`"
