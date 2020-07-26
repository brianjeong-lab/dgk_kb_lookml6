# 2020/06/19 국민은행 키워드가 들어간 문서들의 연관 키워드들
# 현재 170메가정도 사용 ==> Access영역을 10메가 내로 줄일 필요가 있음
SELECT
  A.KEYWORD
  , COUNT(A.KEYWORD) AS cnt
FROM 
  `kb-daas-dev.mart_200723.keyword_list` A
  , (
    SELECT TYPE, ID 
    FROM `kb-daas-dev.mart_200723.keyword_search`
    WHERE 
      KEYWORD_IDX = MOD(ABS(FARM_FINGERPRINT('국민은행')), 4000) 
      AND KEYWORD = '국민은행'
      AND WRITE_DAY = 20200619
    GROUP BY TYPE, ID
  ) B
WHERE
  A.WRITE_DAY = 20200619 AND A.ID = B.ID AND A.TYPE = B.TYPE
GROUP BY
  A.KEYWORD
