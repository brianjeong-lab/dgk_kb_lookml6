INSERT INTO `kb-daas-dev.mart_200722.keyword` (
    CHANNEL, S_NAME, SB_NAME, WRITE_DAY, KEYWORD, CNT, SUM_SCORE
) SELECT
  '' AS CHANNEL
  , '' AS S_NAME
  , '' AS SB_NAME
  , CAST(FORMAT_DATE('%Y%m%d', DATE(A.WRITESTAMP)) as INT64) as day
  , E.name as keyword
  , count(*) as cnt
  , sum(E.salience) as sum_salience
FROM
  `kb-daas-dev.master_200722.keyword_bank_nlp` A
CROSS JOIN
  UNNEST(ENTITIES) AS E
WHERE
  A.CRAWLSTAMP > TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul')
  AND DATE(A.WRITESTAMP) BETWEEN '2020-05-01' AND '2020-06-30'
GROUP BY day, keyword