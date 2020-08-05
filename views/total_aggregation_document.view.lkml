view: total_aggregation_document {
  derived_table: {
    sql: SELECT SUM(CNT_RESULT_DOC) as total

      FROM `kb-daas-dev.mart_200729.aggregation_daily_3`

      WHERE CAST(WRITE_DAY AS STRING) >= {% parameter prmfrom %}
      AND CAST(WRITE_DAY AS STRING) <= {% parameter prmto %}
       ;;
  }



  measure: count {
    type: count
    drill_fields: [detail*]
  }


  dimension: f0_ {
    type: number
    sql: ${TABLE}.f0_ ;;
  }

  set: detail {
    fields: [f0_]
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
