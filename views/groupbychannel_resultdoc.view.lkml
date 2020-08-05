view: groupbychannel_resultdoc {
  derived_table: {
    sql: SELECT CHANNEL, SUM(CNT_RESULT_DOC) AS TOTAL FROM `kb-daas-dev.mart_200729.aggregation_daily_2`
      WHERE WRITE_DAY BETWEEN 20200601 AND 20200630
      GROUP BY CHANNEL
      ORDER BY 2 DESC
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.CHANNEL ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.TOTAL ;;
  }

  set: detail {
    fields: [channel, total]
  }

  measure: cnt {
    type: sum
    sql: COALESCE(${TABLE}.total, 0) ;;
  }


}
