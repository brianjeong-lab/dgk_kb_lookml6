view: tree2 {
  derived_table: {
    sql: select  kpeTable.searchKeyword
        , kpeResult.keyword


       FROM `kb-daas-dev.master_200723.keyword_bank_result` kbr
       ,   (SELECT ID
                  , KPE.keyword as searchKeyword
           FROM `kb-daas-dev.master_200723.keyword_bank_result`
           , UNNEST(KPE) AS kpe
           WHERE DATE(CRAWLSTAMP) < "2020-07-30"
           and kpe.keyword  like {% parameter WORD %}
           group by 1, 2
           limit 20
            ) kpeTable
      , UNNEST(KPE) AS kpeResult
where kbr.id in
           (SELECT ID
            FROM `kb-daas-dev.master_200723.keyword_bank_result`
           , UNNEST(KPE) AS searchkpe
            WHERE DATE(CRAWLSTAMP) < "2020-07-30"
            and searchkpe.keyword  like {% parameter WORD %})
and kbr.id = kpeTable.id

 ;;
  }
  filter: STARTDATE {
    type: date
  }

  filter: ENDDATE {
    type: date
  }

  filter: WORD {
    type: string
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: search_keyword {
    type: string
    sql: ${TABLE}.searchKeyword ;;
  }

  dimension: keyword {
    type: string
    sql: ${TABLE}.keyword ;;
  }

  set: detail {
    fields: [search_keyword, keyword]
  }
}
