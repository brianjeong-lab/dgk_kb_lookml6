view: category_keyword_daily_top_100 {
  derived_table: {
    sql: select
      type,
      channel,
      category,
      keyword,
      PARSE_DATE('%Y%m%d', (CAST(write_day as STRING))) as write_day,
      cnt,
      cnt_doc,
      rank,
      row_num from `mart_200729.category_keyword_daily_top_100`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: doc {
    type: sum
    sql: ${TABLE}.cnt ;;
  }

  measure: cnt_doc {
    type: sum
    sql: ${TABLE}.cnt_doc ;;
  }

  dimension: type {
    type: number
    sql: ${TABLE}.TYPE ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.CHANNEL ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.CATEGORY ;;
  }

  dimension: keyword {
    type: string
    sql: ${TABLE}.KEYWORD ;;
  }

  dimension: write_day {
    type: date
    sql: ${TABLE}.WRITE_DAY ;;
  }

  dimension: cnt {
    type: number
    sql: ${TABLE}.CNT ;;
  }

  dimension: rank {
    type: number
    sql: ${TABLE}.RANK ;;
  }

  dimension: row_num {
    type: number
    sql: ${TABLE}.ROW_NUM ;;
  }

  set: detail {
    fields: [
      type,
      channel,
      category,
      keyword,
      write_day,
      cnt,
      cnt_doc,
      rank,
      row_num
    ]
  }
}
