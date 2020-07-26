# for bank keyword
INSERT INTO `kb-daas-dev.mart_200723.keyword` (
    TYPE, CHANNEL, S_NAME, WRITE_DAY, KEYWORD, CNT, SUM_SCORE
) 
SELECT
  1 AS type
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , K.keyword as keyword
  , count(*) as cnt
  , sum(K.score) as sum_score
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
GROUP BY channel, s_name, day, keyword;


# for corona keyword
INSERT INTO `kb-daas-dev.mart_200723.keyword` (
    TYPE, CHANNEL, S_NAME, WRITE_DAY, KEYWORD, CNT, SUM_SCORE
) 
SELECT
  2 AS type
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , K.keyword as keyword
  , count(*) as cnt
  , sum(K.score) as sum_score
FROM
  `kb-daas-dev.master_200723.keyword_corona_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
GROUP BY channel, s_name, day, keyword;
