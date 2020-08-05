view: groupbycategory_uniquekeyword {
  derived_table: {
    sql: SELECT CATEGORY, SUM(CNT_RESULT_UNIQUE_KEYWORD) AS TOTAL FROM `kb-daas-dev.mart_200729.aggregation_category_daily_3`
      WHERE WRITE_DAY BETWEEN 20200601 AND 20200630
      GROUP BY CATEGORY
      ORDER BY 2 DESC
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

  dimension: total {
    type: number
    sql: ${TABLE}.TOTAL ;;
  }

  set: detail {
    fields: [category, total]
  }

  measure: cnt {
    type: sum
    sql: COALESCE(${TABLE}.total, 0) ;;

  }


}
