#!/bin/bash

IP=$1

curl --location --request POST "http://${IP}:28080/analyze?tasks=d2c,kpe,kse,bnlp,ner" \
--header 'apikey: 5rbeC7bMzbynvbcNqGwOnp5Tll2PUB9B' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'text=(서울=연합뉴스) 조준형 기자 = 2017년 뉴질랜드 주재 한국대사관 간부로 재직하는 동안 대사관 직원을 성추행한 혐의를 받는 현직 외교관 A씨 사건이 최근 한-뉴질랜드 정상 통화에서 거론되는 등 양국 간 외교 현안으로 부상했다.'
