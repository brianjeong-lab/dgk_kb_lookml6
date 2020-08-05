view: keyword_top100 {
  derived_table: {
    sql: SELECT CHANNEL, CATEGORY, KEYWORD, CNT
      FROM `kb-daas-dev.mart_200729.category_keyword_daily_top_100`
      WHERE TYPE = 1
      AND CAST(WRITE_DAY AS STRING) >= {% parameter prmfrom %}
      AND CAST(WRITE_DAY AS STRING) <= {% parameter prmto %}
      AND category = {% parameter prmcat %}
      AND channel = {% parameter prmchn %}
      order by cnt desc
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

  dimension: category {
    type: string
    sql: ${TABLE}.CATEGORY ;;
  }

  dimension: keyword {
    type: string
    sql: ${TABLE}.KEYWORD ;;
  }

  dimension: write_day {
    type: number
    sql: ${TABLE}.WRITE_DAY ;;
  }

  dimension: cnt {
    type: number
    sql: ${TABLE}.CNT ;;
  }

  set: detail {
    fields: [channel, category, keyword, write_day, cnt]
  }

  measure: total {
    type: sum
    sql: COALESCE(${TABLE}.cnt, 0) ;;

  }

  dimension: mydate {
    type: date
    sql: TIMESTAMP(PARSE_DATE('%Y%m%d', FORMAT('%08d',${TABLE}.WRITE_DAY)));;
  }


  filter: prmfrom {
  type: string
  }

  filter: prmto {
  type: string
  }

  filter: prmcat {
    type: string
  }

  filter: prmchn {
    type: string
  }

}
