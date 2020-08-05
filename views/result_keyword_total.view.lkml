view: result_keyword_total {
  derived_table: {
    sql: SELECT SUM(CNT_RESULT_KEYWORD) as total

      FROM `kb-daas-dev.mart_200729.aggregation_daily_3`

      WHERE CAST(WRITE_DAY AS STRING) >= {% parameter prmfrom %}
      AND CAST(WRITE_DAY AS STRING) <= {% parameter prmto %}

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

  filter: prmto {
    type: string
  }

  filter: prmfrom {
    type: string
  }


}
