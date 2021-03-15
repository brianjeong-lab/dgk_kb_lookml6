-- Bigquery History


-- INSERT INTO `mart_200729.keyword_bank_result_agg_remove_docid` (docid)  
-- SELECT DOCID FROM `master_200729.keyword_bank` WHERE
-- DATE(D_CRAWLSTAMP) > "2020-05-31" and D_URL like 'https://cafe.naver.com/haruha2016/%';


-- SELECT DOCID FROM `master_200729.keyword_bank` WHERE
-- DATE(D_CRAWLSTAMP) > "2020-05-31" and D_URL like 'https://cafe.naver.com/haruha2016/%';


-- INSERT INTO `mart_200729.keyword_bank_result_agg_remove_docid` (docid, keyword)
-- SELECT
--     distinct docid
--     , "오픈뱅킹"
-- FROM
--   `kb-daas-dev.mart_200729.keyword_bank_result_agg_only_bank` TA, 
--   UNNEST(TA.KPE) TB
-- WHERE
--   TA.CRAWLSTAMP BETWEEN TIMESTAMP_SUB(TIMESTAMP '2020-06-01', INTERVAL 1 DAY) and TIMESTAMP_ADD(TIMESTAMP '2020-06-30', INTERVAL 1 DAY)
--   AND DATE(TA.WRITESTAMP, 'Asia/Seoul') >= '2020-06-01'
--   AND DATE(TA.WRITESTAMP, 'Asia/Seoul') <= '2020-06-30'
--   AND TB.keyword not in (SELECT keyword FROM `mart_200729.filter`)
--   AND length(TB.keyword) <= 10
--   AND TA.channel != "뉴스"
--   AND 
--   TA.DOCID not in (SELECT docid FROM `mart_200729.keyword_bank_result_agg_remove_docid`)
--   AND TB.keyword = "오픈뱅킹";


SELECT
    DATE(TA.WRITESTAMP, 'Asia/Seoul') as day, 
    TA.channel,
    (CASE 
      WHEN ARRAY_LENGTH(TA.D2C) > 0 THEN TA.D2C[SAFE_OFFSET(0)].label
      ELSE NULL
    END) AS CATEGORY ,
    TB.keyword AS keyword,
    TB.score AS score,
    TA.TITLE,
    1 AS cnt
FROM
  `kb-daas-dev.mart_200729.keyword_bank_result_agg_only_bank` TA, 
  UNNEST(TA.KPE) TB
WHERE
  TA.CRAWLSTAMP BETWEEN TIMESTAMP_SUB(TIMESTAMP '2020-06-01', INTERVAL 1 DAY) and TIMESTAMP_ADD(TIMESTAMP '2020-06-30', INTERVAL 1 DAY)
  AND DATE(TA.WRITESTAMP, 'Asia/Seoul') >= '2020-06-01'
  AND DATE(TA.WRITESTAMP, 'Asia/Seoul') <= '2020-06-30'
  AND TB.keyword not in (SELECT keyword FROM `mart_200729.filter`)
  AND length(TB.keyword) <= 10
  AND TA.channel != "뉴스"
  AND 
  TA.DOCID not in (SELECT docid FROM `mart_200729.keyword_bank_result_agg_remove_docid`)
  AND TB.keyword = "KB마이핏";
  
  
-- INSERT INTO `mart_200729.filter` (type, keyword) values 
-- ('1', 'heo33hetistorycom'), 
-- ('1', '20191229'),
-- ('1', 'eviewktcom'),
-- ('1', '디지털데일리'),
-- ('1', '고정현');
