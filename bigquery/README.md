# Partition 현황조회
```
#legacySQL
SELECT
  partition_id,
  last_modified_time
FROM
  [{dataset}.{table_name}$__PARTITIONS_SUMMARY__]
```


# UNEST Query 샘플
```
SELECT 
    K.keyword as keyword
    , count(*) as cnt
FROM 
    `kb-daas-dev.master.keyword_bank_result` 
CROSS JOIN 
    UNNEST(KPE) AS K  
WHERE 
    DATE(CRAWLSTAMP) > "2020-05-20" 
    and K.category = 'NNP' 
group by 
    keyword 
order by 
    cnt desc
```

# 최신의 데이타만 찾는 쿼리
```
SELECT 
   A.* 
FROM  
  `kb-daas-dev.master.keyword_bank_result` A, 
  (select ID, MAX(RESPONSE.proc_time) as proc_time from `kb-daas-dev.master.keyword_bank_result` where DATE(CRAWLSTAMP) > "2020-05-20" and RESPONSE.status_code = 200 GROUP BY ID ) B
WHERE
  DATE(A.CRAWLSTAMP) > "2020-05-20" AND A.ID = B.ID AND A.RESPONSE.proc_time = B.proc_time
```

# 중복데이타 찾기
```
SELECT 
   A.ID, A.RESPONSE.proc_time
FROM  
  `kb-daas-dev.master.keyword_bank_result` A, 
  (SELECT 
    ID, proc_time 
   FROM (
      select ID, MIN(RESPONSE.proc_time) as proc_time, count(*) as cnt 
      from `kb-daas-dev.master.keyword_bank_result` 
      where DATE(CRAWLSTAMP) > "2020-05-20" and RESPONSE.status_code = 200 
      GROUP BY ID
    ) BB
    WHERE cnt > 1
  ) B
WHERE
  DATE(A.CRAWLSTAMP) > "2020-05-20" AND A.ID = B.ID AND A.RESPONSE.proc_time = B.proc_time
```

# 중복데이타 삭제
```
DELETE FROM `kb-daas-dev.master.keyword_bank_result` A
WHERE
  DATE(A.CRAWLSTAMP) > "2020-05-20"
  AND CONCAT(CAST(A.ID as string), '_', CAST(A.RESPONSE.proc_time as string)) IN (
    SELECT 
       CONCAT(CAST(BB.ID as string), '_', CAST(BB.proc_time as string))
     FROM (
        select ID, MIN(RESPONSE.proc_time) as proc_time, count(*) as cnt 
        from `kb-daas-dev.master.keyword_bank_result` 
        where DATE(CRAWLSTAMP) > "2020-05-20" and RESPONSE.status_code = 200 
        GROUP BY ID
      ) BB
     WHERE cnt > 1
  ) 
```

# 원본 데이터와 KB-STA 데이터 같이 보기
```
SELECT a.id
     , b.origin_id
     , a.ss
     , d_content
     , a.d2c
  FROM `kb-daas-dev.master.keyword_bank_result` A
     , (select id as origin_id 
             , d_content
          from `kb-daas-dev.master.keyword_bank`
         where DATE(d_CRAWLSTAMP) > "2020-05-20" ) b
 WHERE DATE(A.CRAWLSTAMP) > "2020-05-20" 
   and a.id = b.origin_id
```
# UNNEST를 이용하여 키워드로 filltering 하기
```
select * FROM `kb-daas-dev.master.keyword_bank_result` a
cross join unnest(d2c) b
where  DATE(CRAWLSTAMP) > "2020-05-20"
and b.label = '여행'
```

# YouTube Table Information
```
[https://cloud.google.com/bigquery-transfer/docs/youtube-channel-transformation](https://cloud.google.com/bigquery-transfer/docs/youtube-channel-transformation)

무료라 그런지 쓸만한 데이터는 Likes, DisLikes, Share 정보정도밖에 없음 (당연한건가?)
심지어 채널ID 및 VideoID도 암호화되어 있어서.... 디테일하게 뭔가 보여줄순 
````

# 시나리오 EDA SQL (데이터를 좀 더 봐야할듯)
```
SELECT id
     , channel
     , s_name
     , sb_name
     , d_title
     , d_content
     , d_url
     , json_data
  FROM `kb-daas-dev.master_200723.keyword_channel_addtion` a
  WHERE DATE( D_CRAWLSTAMP ) > "2020-07-01"
    and id in (SELECT id
                FROM `kb-daas-dev.master_200723.keyword_channel_addtion_result` a
                cross join unnest(kpe) b
                cross join unnest(d2c) c
                WHERE DATE(WRITESTAMP) > "2020-07-01"
                  and b.keyword like '%계좌%'
                  and (c.label   like '%경제%' or c.label   like '%사회%')
                group by id)
;

````

#첫글자가 소문자 영문일 경우 ''로 치환
````
select sb_name
     , case when REGEXP_CONTAINS(sb_name, '[a-z]')  = true then trim(REGEXP_REPLACE(SB_NAME, '[a-z]', ''))
       else sb_name end replace_name2
  from master_200729.keyword_bank
 where date(D_CRAWLSTAMP) > "2020-06-01"
   and channel = "뉴스"
 limit 100
 ````
 데이터 확인 후 업데이트 문장 실행
 
 ````
 UPDATE master_200729.keyword_bank
   SET sb_name = case when REGEXP_CONTAINS(sb_name, '[a-z]')  = true then trim(REGEXP_REPLACE(SB_NAME, '[a-z]', ''))
                 else sb_name end
WHERE TRUE
 ````
