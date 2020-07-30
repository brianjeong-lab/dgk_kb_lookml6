connection: "kb-daas-dev"

# include all the views
include: "/views/**/*.view"

datagroup: dgk_kb_lookml6_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: dgk_kb_lookml6_default_datagroup

explore: keyword_bank {}

explore: keyword_bank_result {
  join: keyword_bank_result__kpe {
    view_label: "Keyword Bank Result: Kpe"
    sql: LEFT JOIN UNNEST(${keyword_bank_result.kpe}) as keyword_bank_result__kpe ;;
    relationship: one_to_many
  }

  join: keyword_bank_result__d2_c {
    view_label: "Keyword Bank Result: D2c"
    sql: LEFT JOIN UNNEST(${keyword_bank_result.d2_c}) as keyword_bank_result__d2_c ;;
    relationship: one_to_many
  }

  join: keyword_bank_result__kse {
    view_label: "Keyword Bank Result: Kse"
    sql: LEFT JOIN UNNEST(${keyword_bank_result.kse}) as keyword_bank_result__kse ;;
    relationship: one_to_many
  }

  join: keyword_bank_result__response {
    view_label: "Keyword Bank Result: Response"
    sql: LEFT JOIN UNNEST([${keyword_bank_result.response}]) as keyword_bank_result__response ;;
    relationship: one_to_one
  }
}

explore: keyword_channel {}

explore: keyword_channel_addtion {}

explore: keyword_channel_addtion_result {
  join: keyword_channel_addtion_result__kpe {
    view_label: "Keyword Channel Addtion Result: Kpe"
    sql: LEFT JOIN UNNEST(${keyword_channel_addtion_result.kpe}) as keyword_channel_addtion_result__kpe ;;
    relationship: one_to_many
  }

  join: keyword_channel_addtion_result__d2_c {
    view_label: "Keyword Channel Addtion Result: D2c"
    sql: LEFT JOIN UNNEST(${keyword_channel_addtion_result.d2_c}) as keyword_channel_addtion_result__d2_c ;;
    relationship: one_to_many
  }

  join: keyword_channel_addtion_result__kse {
    view_label: "Keyword Channel Addtion Result: Kse"
    sql: LEFT JOIN UNNEST(${keyword_channel_addtion_result.kse}) as keyword_channel_addtion_result__kse ;;
    relationship: one_to_many
  }

  join: keyword_channel_addtion_result__response {
    view_label: "Keyword Channel Addtion Result: Response"
    sql: LEFT JOIN UNNEST([${keyword_channel_addtion_result.response}]) as keyword_channel_addtion_result__response ;;
    relationship: one_to_one
  }
}

explore: keyword_channel_result {
  join: keyword_channel_result__kpe {
    view_label: "Keyword Channel Result: Kpe"
    sql: LEFT JOIN UNNEST(${keyword_channel_result.kpe}) as keyword_channel_result__kpe ;;
    relationship: one_to_many
  }

  join: keyword_channel_result__d2_c {
    view_label: "Keyword Channel Result: D2c"
    sql: LEFT JOIN UNNEST(${keyword_channel_result.d2_c}) as keyword_channel_result__d2_c ;;
    relationship: one_to_many
  }

  join: keyword_channel_result__kse {
    view_label: "Keyword Channel Result: Kse"
    sql: LEFT JOIN UNNEST(${keyword_channel_result.kse}) as keyword_channel_result__kse ;;
    relationship: one_to_many
  }

  join: keyword_channel_result__response {
    view_label: "Keyword Channel Result: Response"
    sql: LEFT JOIN UNNEST([${keyword_channel_result.response}]) as keyword_channel_result__response ;;
    relationship: one_to_one
  }
}

explore: keyword_corona {}

explore: keyword_corona_result {
  join: keyword_corona_result__kpe {
    view_label: "Keyword Corona Result: Kpe"
    sql: LEFT JOIN UNNEST(${keyword_corona_result.kpe}) as keyword_corona_result__kpe ;;
    relationship: one_to_many
  }

  join: keyword_corona_result__d2_c {
    view_label: "Keyword Corona Result: D2c"
    sql: LEFT JOIN UNNEST(${keyword_corona_result.d2_c}) as keyword_corona_result__d2_c ;;
    relationship: one_to_many
  }

  join: keyword_corona_result__kse {
    view_label: "Keyword Corona Result: Kse"
    sql: LEFT JOIN UNNEST(${keyword_corona_result.kse}) as keyword_corona_result__kse ;;
    relationship: one_to_many
  }

  join: keyword_corona_result__response {
    view_label: "Keyword Corona Result: Response"
    sql: LEFT JOIN UNNEST([${keyword_corona_result.response}]) as keyword_corona_result__response ;;
    relationship: one_to_one
  }
}
