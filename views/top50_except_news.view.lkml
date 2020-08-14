view: top50_except_news {
  derived_table: {
    sql: select keyword, sum(cnt) as Total
      FROM `kb-daas-dev.mart_200729.category_keyword_daily_top_100`
      where write_day between 20200601 and 20200630
      and type = 1
      and channel != '뉴스'
      group by keyword
      order by Total desc limit 30
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: keyword {
    type: string
    sql: ${TABLE}.keyword ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.Total ;;
  }

  set: detail {
    fields: [keyword, total]
  }

  measure: cnt {
    type: sum
    sql: COALESCE(${TABLE}.total, 0) ;;
  }
}
