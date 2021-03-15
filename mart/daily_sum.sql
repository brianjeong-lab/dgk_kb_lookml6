INSERT INTO `kb-daas-dev.mart_200723.daily_sum` (
    TYPE, CHANNEL, S_NAME, WRITE_DAY, CNT_KEYWORD, SUM_SCORE_KEYWORD, CNT_DOC
) 
SELECT
  1 AS type
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , count(*) as cnt
  , sum(K.score) as sum_score
  , count(distinct ID) as cnt_doc
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-01' AND '2020-06-30'
GROUP BY channel, s_name, day

INSERT INTO `kb-daas-dev.mart_200723.daily_sum` (
    TYPE, CHANNEL, S_NAME, WRITE_DAY, CNT_KEYWORD, SUM_SCORE_KEYWORD, CNT_DOC
) 
SELECT
  2 AS type
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , count(*) as cnt
  , sum(K.score) as sum_score
  , count(distinct ID) as cnt_doc
FROM
  `kb-daas-dev.master_200723.keyword_corona_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-01' AND '2020-06-30'
GROUP BY channel, s_name, day
