INSERT INTO `kb-daas-dev.mart_200723.keyword_2_more` (
    TYPE, CHANNEL, S_NAME, WRITE_DAY, KEYWORD, CNT, SUM_SCORE
) 
SELECT
  TYPE
  , CHANNEL
  , S_NAME
  , WRITE_DAY
  , KEYWORD
  , CNT
  , SUM_SCORE
FROM
  `kb-daas-dev.mart_200723.keyword_sum`
WHERE
  CNT > 1
