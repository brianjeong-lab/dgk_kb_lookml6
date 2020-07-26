# INSERT
INSERT INTO `kb-daas-dev.mart_200723.keyword_search` (
    TYPE, ID, CHANNEL, S_NAME, SB_NAME, WRITE_DAY, KEYWORD, KEYWORD_IDX
) 
SELECT
  1 AS type
  , A.ID AS id
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , A.SB_NAME AS sb_name
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP, 'Asia/Seoul')) as INT64) as day
  , K.keyword AS keyword
  , MOD(ABS(FARM_FINGERPRINT(K.keyword)), 4000) AS KEYWORD_IDX
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
;

# Sample
SELECT * FROM `kb-daas-dev.mart_200723.keyword_search`
WHERE KEYWORD_IDX = MOD(ABS(FARM_FINGERPRINT('국민은행')), 4000) and KEYWORD = '국민은행'
