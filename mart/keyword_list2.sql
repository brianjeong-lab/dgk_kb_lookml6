INSERT INTO `kb-daas-dev.mart_200723.keyword_list2` (
    TYPE, ID, CHANNEL, S_NAME, SB_NAME, WRITESTAMP, KEYWORD
) 
SELECT
  1 AS type
  , A.ID AS id
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , A.SB_NAME AS sb_name
  , A.WRITESTAMP as day
  , K.keyword AS keyword
FROM
  `kb-daas-dev.master_200723.keyword_bank_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
;

INSERT INTO `kb-daas-dev.mart_200723.keyword_list2` (
    TYPE, ID, CHANNEL, S_NAME, SB_NAME, WRITESTAMP, KEYWORD
) 
SELECT
  2 AS type
  , A.ID AS id
  , A.CHANNEL AS channel
  , A.S_NAME AS s_name
  , A.SB_NAME AS sb_name
  , A.WRITESTAMP as day
  , K.keyword AS keyword
FROM
  `kb-daas-dev.master_200723.keyword_corona_result` A
CROSS JOIN
  UNNEST(KPE) AS K
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-31' AND '2020-06-30'
;
