view: groupbycategory_trend_keyword {
  derived_table: {
    sql: SELECT A.CATEGORY, A.CNT_RESULT_KEYWORD, A.WRITE_DAY
      FROM `kb-daas-dev.mart_200729.aggregation_category_daily_3` A
      WHERE A.CATEGORY in
        (SELECT B.CATEGORY
      FROM `kb-daas-dev.mart_200729.aggregation_category_daily_3` B
      WHERE B.WRITE_DAY BETWEEN 20200601 AND 20200630
      GROUP BY B.CATEGORY
      ORDER BY SUM(B.CNT_RESULT_KEYWORD) desc LIMIT 10
        )
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: category {
    type: string
    sql: ${TABLE}.CATEGORY ;;
  }

  dimension: cnt_result_keyword {
    type: number
    sql: ${TABLE}.CNT_RESULT_KEYWORD ;;
  }

  dimension: write_day {
    type: number
    sql: ${TABLE}.WRITE_DAY ;;
  }

  set: detail {
    fields: [category, cnt_result_keyword, write_day]
  }

  measure: cnt {
    type: sum
    sql: COALESCE(${TABLE}.CNT_RESULT_KEYWORD, 0) ;;
  }

  dimension: mydate {
    type: date
    sql: TIMESTAMP(PARSE_DATE('%Y%m%d', FORMAT('%08d',${TABLE}.WRITE_DAY)));;
  }
}
