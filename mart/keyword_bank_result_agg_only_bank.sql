
-- keyword_bank_result_agg_only_bank
[
  {
    "description": "Title",
    "mode": "REQUIRED",
    "name": "TITLE",
    "type": "STRING"
  },
  {
    "description": "Content",
    "mode": "REQUIRED",
    "name": "CONTENT",
    "type": "STRING"
  },
  {
    "description": "URL",
    "mode": "REQUIRED",
    "name": "URL",
    "type": "STRING"
  },
  {
    "description": "Document ID",
    "mode": "REQUIRED",
    "name": "DOCID",
    "type": "INTEGER"
  },
  {
    "description": "Channel Info",
    "mode": "REQUIRED",
    "name": "CHANNEL",
    "type": "STRING"
  },
  {
    "description": "Service Name",
    "mode": "REQUIRED",
    "name": "S_NAME",
    "type": "STRING"
  },
  {
    "description": "SB Name",
    "mode": "REQUIRED",
    "name": "SB_NAME",
    "type": "STRING"
  },
  {
    "description": "Crawler Time",
    "mode": "REQUIRED",
    "name": "CRAWLSTAMP",
    "type": "TIMESTAMP"
  },
  {
    "description": "Written Time",
    "mode": "REQUIRED",
    "name": "WRITESTAMP",
    "type": "TIMESTAMP"
  },
  {
    "description": "Sentences",
    "mode": "REPEATED",
    "name": "SS",
    "type": "STRING"
  },
  {
    "description": "Document to Categorization",
    "mode": "REPEATED",
    "name": "D2C",
    "type": "RECORD",
    "fields": [
            {
                "name": "label",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "score",
                "type": "FLOAT",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "description": "Keyword ...",
    "mode": "REPEATED",
    "name": "KPE",
    "type": "RECORD",
    "fields": [
            {
                "name": "keyword",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "category",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "score",
                "type": "FLOAT",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "description": "Key Sentens Extration",
    "mode": "REPEATED",
    "name": "KSE",
    "type": "RECORD",
    "fields": [
            {
                "name": "idx",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "score",
                "type": "FLOAT",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "description": "Response from KB STA",
    "mode": "NULLABLE",
    "name": "RESPONSE",
    "type": "RECORD",
    "fields": [
            {
                "name": "status_code",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "proc_time",
                "type": "FLOAT",
                "mode": "REQUIRED"
            },
            {
                "name": "err_msg",
                "type": "STRING",
                "mode": "NULLABLE"
            }
    ]
  },
  {
    "description": "Processing Time",
    "mode": "REQUIRED",
    "name": "PROCSTAMP",
    "type": "TIMESTAMP"
  }
]



-- 은행키워드 검색 데이터 입력

 INSERT INTO `kb-daas-dev.mart_200729.keyword_bank_result_agg_only_bank`
  (TITLE, CONTENT, URL, DOCID, CHANNEL, S_NAME, SB_NAME, CRAWLSTAMP, WRITESTAMP, SS, D2C, KPE, KSE, RESPONSE, PROCSTAMP)
            SELECT
              TA.D_TITLE
              , TA.D_CONTENT
              , TA.D_URL
              , TA.DOCID
              , TA.CHANNEL
              , TA.S_NAME
              , TA.SB_NAME
              , TA.D_CRAWLSTAMP
              , TA.D_WRITESTAMP
              , TC.SS
              , TC.D2C
              , TC.KPE
              , TC.KSE
              , TC.RESPONSE
              , TC.PROCSTAMP
            FROM
              `kb-daas-dev.master_200729.keyword_bank` TA
               , `kb-daas-dev.master_200729.keyword_bank_result` TC
               WHERE
                  TA.DOCID IN (
                    SELECT
                      distinct A.DOCID
                    FROM
                      `kb-daas-dev.master_200729.keyword_bank_result` A,
                      UNNEST(A.KPE) K
                    WHERE
                      A.CRAWLSTAMP > TIMESTAMP_SUB(TIMESTAMP '2020-06-01', INTERVAL 1 DAY)
                      AND (K.keyword = 'KB국민은행' or K.keyword = '국민은행' or K.keyword = 'KB은행' or K.keyword = 'KBBANK' or K.keyword = '신한은행' or K.keyword = '우리은행' or K.keyword = '하나은행')
                  ) 
                  AND TA.D_CRAWLSTAMP > TIMESTAMP_SUB(TIMESTAMP '2020-06-01', INTERVAL 1 DAY)
                  AND TC.CRAWLSTAMP > TIMESTAMP_SUB(TIMESTAMP '2020-06-01', INTERVAL 1 DAY)
                  AND TA.DOCID = TC.DOCID
