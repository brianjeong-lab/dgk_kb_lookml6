# for bank
INSERT INTO `kb-daas-dev.mart_200723.category` (
    TYPE, CHANNEL, CATEGORY, KEYWORD, WRITE_DAY, CNT, SUM_SCORE, CNT_DOC
) 
SELECT
  1 AS type
  , A.CHANNEL AS channel
  , A.D2C[SAFE_OFFSET(0)].label AS category
  , K.keyword AS keyword
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , COUNT(K.keyword) AS cnt
  , SUM(K.score) AS sum_score 
  , COUNT(distinct A.ID) AS cnt_doc
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
GROUP BY
  channel, category, keyword, day
;

# for corona
INSERT INTO `kb-daas-dev.mart_200723.category` (
    TYPE, CHANNEL, CATEGORY, KEYWORD, WRITE_DAY, CNT, SUM_SCORE, CNT_DOC
) 
SELECT
  2 AS type
  , A.CHANNEL AS channel
  , A.D2C[SAFE_OFFSET(0)].label AS category
  , K.keyword AS keyword
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , COUNT(K.keyword) AS cnt
  , SUM(K.score) AS sum_score 
  , COUNT(distinct A.ID) AS cnt_doc
FROM
  `kb-daas-dev.master_200723.keyword_corona_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
GROUP BY
  channel, category, keyword, day
;
