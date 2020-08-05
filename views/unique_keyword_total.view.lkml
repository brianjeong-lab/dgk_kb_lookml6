view: unique_keyword_total {
  derived_table: {
    sql: SELECT SUM(CNT_RESULT_UNIQUE_KEYWORD) as total
      FROM `kb-daas-dev.mart_200729.aggregation_daily_3`
      WHERE WRITE_DAY BETWEEN 20200601 AND 20200630
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  set: detail {
    fields: [total]
  }

  measure: cnt {
    type: sum
    sql: COALESCE(${TABLE}.total, 0) ;;
  }

}
