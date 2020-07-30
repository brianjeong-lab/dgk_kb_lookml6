view: keyword_channel_addtion {
  sql_table_name: `master_200723.keyword_channel_addtion`
    ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.ID ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.CHANNEL ;;
  }

  dimension: d_content {
    type: string
    sql: ${TABLE}.D_CONTENT ;;
  }

  dimension_group: d_crawlstamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.D_CRAWLSTAMP ;;
  }

  dimension: d_title {
    type: string
    sql: ${TABLE}.D_TITLE ;;
  }

  dimension: d_url {
    type: string
    sql: ${TABLE}.D_URL ;;
  }

  dimension: d_writer {
    type: string
    sql: ${TABLE}.D_WRITER ;;
  }

  dimension: d_writer_id {
    type: string
    sql: ${TABLE}.D_WRITER_ID ;;
  }

  dimension_group: d_writestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.D_WRITESTAMP ;;
  }

  dimension: docid {
    type: number
    value_format_name: id
    sql: ${TABLE}.DOCID ;;
  }

  dimension: json_data {
    type: string
    sql: ${TABLE}.JSON_DATA ;;
  }

  dimension: s_name {
    type: string
    sql: ${TABLE}.S_NAME ;;
  }

  dimension: s_seq {
    type: number
    sql: ${TABLE}.S_SEQ ;;
  }

  dimension: sb_name {
    type: string
    sql: ${TABLE}.SB_NAME ;;
  }

  measure: count {
    type: count
    drill_fields: [id, s_name, sb_name]
  }
}
