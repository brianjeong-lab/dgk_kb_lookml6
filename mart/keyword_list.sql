# for bank
INSERT INTO `kb-daas-dev.mart_200723.keyword_list` (
    TYPE, ID, CHANNEL, S_NAME, SB_NAME, WRITE_DAY, KEYWORD
) 
SELECT
  1 AS type
  , A.ID AS id
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , A.SB_NAME AS sb_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , K.keyword AS keyword
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
;

# for corona
INSERT INTO `kb-daas-dev.mart_200723.keyword_list` (
    TYPE, ID, CHANNEL, S_NAME, SB_NAME, WRITE_DAY, KEYWORD
) 
SELECT
  2 AS type
  , A.ID AS id
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , A.SB_NAME AS sb_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , K.keyword AS keyword
FROM
  `kb-daas-dev.master_200723.keyword_corona_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
;
