view: aggregation_daily_2 {
  derived_table: {
    sql: SELECT CHANNEL, CNT_RESULT_DOC, CNT_RESULT_KEYWORD, CNT_RESULT_UNIQUE_KEYWORD, WRITE_DAY
       FROM `kb-daas-dev.mart_200729.aggregation_daily_2`
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

  dimension: cnt_result_doc {
    type: number
    sql: ${TABLE}.CNT_RESULT_DOC ;;
  }

  dimension: cnt_result_keyword {
    type: number
    sql: ${TABLE}.CNT_RESULT_KEYWORD ;;
  }

  dimension: cnt_result_unique_keyword {
    type: number
    sql: ${TABLE}.CNT_RESULT_UNIQUE_KEYWORD ;;
  }

  dimension: write_day {
    type: number
    sql: ${TABLE}.WRITE_DAY ;;
  }

  set: detail {
    fields: [channel, cnt_result_doc, cnt_result_keyword, cnt_result_unique_keyword, write_day]
  }

  dimension: mydate {
    type: date
    sql: TIMESTAMP(PARSE_DATE('%Y%m%d', FORMAT('%08d',${TABLE}.WRITE_DAY)));;
  }

  measure: cnt_doc{
    type: sum
    sql: COALESCE(${TABLE}.CNT_RESULT_DOC, 0) ;;
    }

  measure: cnt_keyword{
    type: sum
    sql: COALESCE(${TABLE}.CNT_RESULT_KEYWORD, 0) ;;
  }

  measure: cnt_ukeyword{
    type: sum
    sql: COALESCE(${TABLE}.CNT_RESULT_UNIQUE_KEYWORD, 0) ;;
  }


}
