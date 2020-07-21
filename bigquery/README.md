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
