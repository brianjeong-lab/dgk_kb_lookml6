import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_messages
import requests
import csv
import sys
import json
import time
import re

# import Python logging module.
import logging

project_id = 'kb-daas-dev' # your project ID
#dataset_id = 'master' # your dataset ID
#table_id = 'keyword_bank_result' # your table ID

# for Standalone Test
options = {
    'project': project_id,
    'runner:': 'DirectRunner'
    }

options = PipelineOptions(flags=[], **options)  # create and set your PipelineOptions

class DefaultResponse:
    text = ''
    status_code = 0
    proc_time = 0
    err_msg = ''

# 불필요한 특수문자 제거 전처리 함수
def cleasing_contents(contents):
    remove_target_char = ["&", "="] # 본문에서 제거해야할 특수문자
    for char in remove_target_char:
        contents = contents.replace(char, " ")

    #print(time.time(), "cleasing_contents", contents)

    return contents

def cleansing_row(row):
    row["D_CONTENT"] = cleasing_contents(row["D_CONTENT"])
    return row

# ai api 콜 함수 # content에 분석할 내용 담기
def call_kbsta_api(content):
    # AWS
    #url = "http://3.34.18.1:8080/analyze"
    # GCP
    #url = "http://34.64.172.194:28080/analyze"
    # LB
    url = "http://35.241.3.78:8080/analyze"
    querystring = {"tasks":"d2c,kpe,kse,bnlp,ner"}
    #body = "text=" + re.sub(r'[\xa0]', '', content)
    body = "text=" + re.sub('[_=#/?:$}&^·\xa0]', '', content)
    #body = "text=" + content
    #print("body", body)
    #print(content[1111], content[1112])
    #print(hex(ord(content[1111])), hex(ord(content[1112])))
    body = body.encode(encoding='utf-8')
    headers = {
        'apikey': "5rbeC7bMzbynvbcNqGwOnp5Tll2PUB9B",
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
        }
    
    start = time.time()

    try:
        response = requests.request("POST", url, data=body, headers=headers, params=querystring)
        response.proc_time = time.time() - start
        response.err_msg = ''

        #print("Response", response)
        #print("Text", response.text)
   
    except Exception as e:
        logging.warning("RESPONSE Error {}".format(e))

        response = DefaultResponse()
        response.text = ''
        response.status_code = 999
        response.proc_time = time.time() - start
        response.err_msg = str(e)
    
    return response


def call_kbsta_api_with_json(docid, content):

    response = call_kbsta_api(content)

    # JSON 디코딩
    try:
        if response.status_code != 999:
            json_array = json.loads(response.text)
        else:
            json_array = {}

        json_array["response"] = { "status_code" : response.status_code , "proc_time" : response.proc_time, "err_msg" : response.err_msg }
        
        return json_array
    except Exception as e:
        logging.warning("call_kbsta_api Error docid:{} {} {} ".format(docid, e, len(content)))
        json_array = {}
        json_array["response"] = { "status_code" : 900 , "proc_time" : 0, "err_msg" : str(e) }
        return json_array

# formated
def convert_kbsta_formatted_map(row):

    ss = []
    d2c = []
    kpe = []
    kse = []
    response = None

    if row['D_RESULT'] is not None:

        result = row['D_RESULT']

        if 'sentence' in result.keys():
            sentences = result['sentence']
            for sentence in sentences:
                ss.append(sentence["raw"])

        if 'd2c' in result.keys():
            d2c = result['d2c'] 

        if 'kse' in result.keys():
            kse = result['kse']

        if 'kpe' in result.keys():
            kpe = result['kpe']
        
        if 'response' in result.keys():
            response = result['response']

    yield {
        'DOCID':row['DOCID']
        , 'CHANNEL':row['CHANNEL']
        , 'S_NAME':row['S_NAME']
        , 'SB_NAME':row['SB_NAME']
        , 'CRAWLSTAMP':row['D_CRAWLSTAMP']
        , 'WRITESTAMP':row['D_WRITESTAMP']
        , 'SS': ss
        , 'D2C' : d2c
        , 'KPE' : kpe
        , 'KSE' : kse
        , 'RESPONSE' : response
        , 'PROCSTAMP' : time.time()
    }

def printer(element):
    print()
    print("printer:")
    print(element)
    print()

def fileread(filename):
    f = open(filename, "r")
    contents = read = f.read()
    f.close()
    return contents

def count_ones(word_ones):
    #print(word_ones)
    (word, ones) = word_ones
    return (word, sum(ones))

class DateExtractor(beam.DoFn):
    def process(self, data_item):
        #print(self, 'process', data_item)
        return [data_item]

def main(argv):

    #schema = fileread('keyword_bank_result.schema')
    #print("schema", schema)

    dataset = "master_200729"
    table = "keyword_bank"
    year = "2020"
    month = "06"
    day = sys.argv[1]

    # for test
    pipeline = beam.Pipeline(options=options)

    query=f"""
SELECT 
    A.DOCID
    , A.CHANNEL
    , A.S_NAME
    , A.SB_NAME
    , A.D_CRAWLSTAMP
    , A.D_WRITESTAMP
    , A.D_CONTENT 
FROM (
  SELECT 
      A.DOCID
      , A.CHANNEL
      , A.S_NAME
      , A.SB_NAME
      , A.D_CRAWLSTAMP
      , A.D_WRITESTAMP
      , A.D_CONTENT 
      , B.DOCID AS BID
  FROM
  (
    SELECT 
        DOCID
        , CHANNEL
        , S_NAME
        , SB_NAME
        , D_CRAWLSTAMP
        , D_WRITESTAMP
        , D_CONTENT 
    FROM 
        `{dataset}.{table}` 
    WHERE 
        D_CRAWLSTAMP BETWEEN TIMESTAMP('{year}-{month}-{day} 00:00:00', 'Asia/Seoul') 
        AND TIMESTAMP_ADD(TIMESTAMP('{year}-{month}-{day} 00:00:00', 'Asia/Seoul'), INTERVAL 1 DAY)
  ) A 
  LEFT OUTER JOIN (
      SELECT 
        DOCID
      FROM 
          `{dataset}.{table}_result` 
      WHERE 
          CRAWLSTAMP BETWEEN TIMESTAMP('{year}-{month}-{day} 00:00:00', 'Asia/Seoul') 
          AND TIMESTAMP_ADD(TIMESTAMP('{year}-{month}-{day} 00:00:00', 'Asia/Seoul'), INTERVAL 1 DAY)
  ) B
  ON A.DOCID = B.DOCID
) A
WHERE
  A.BID IS NULL
  -- AND LENGTH(A.D_CONTENT) != 100000
LIMIT 250
                """

    query=f"""
SELECT 
    A.DOCID
    , A.CHANNEL
    , A.S_NAME
    , A.SB_NAME
    , A.D_CRAWLSTAMP
    , A.D_WRITESTAMP
    , A.D_CONTENT 
FROM 
    `master_200729.keyword_bank` A
WHERE 
    D_CRAWLSTAMP BETWEEN TIMESTAMP('2020-06-09 22:00:00', 'Asia/Seoul') 
    AND TIMESTAMP_ADD(TIMESTAMP('2020-06-09 22:00:00', 'Asia/Seoul'), INTERVAL 1 HOUR)
    AND DOCID = 7419868596
"""

    raw_data = (pipeline
        | 'Read Data From BigQuery' >> beam.io.Read(
            beam.io.BigQuerySource(
                query=query,
                project=project_id,
                use_standard_sql=True)
        )
    )

    # raw_data = (pipeline 
    #     | beam.Create(
    #         [
    #             'a',
    #             "b",
    #             'c',
    #             'e',
    #         ]
    #     )
    # )

    processing_data = (raw_data
        #| 'Testing' >> beam.ParDo(DateExtractor())
        #| 'Testing' >> beam.Map(cleasing_contents)
        #| 'Testing 2' >> beam.GroupByKey()
        #| 'Testing 3' >> beam.Map(count_ones)
        | 'Cleansing' >> beam.Map(lambda row: {'DOCID':row['DOCID'], 'CHANNEL':row['CHANNEL'], 'S_NAME':row['S_NAME'], 'SB_NAME':row['SB_NAME'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'D_CONTENT':cleasing_contents(row['D_CONTENT'])})
        | 'Call with KB STA API' >> beam.Map(lambda row: {'DOCID':row['DOCID'], 'CHANNEL':row['CHANNEL'], 'S_NAME':row['S_NAME'], 'SB_NAME':row['SB_NAME'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'D_RESULT':call_kbsta_api_with_json(row['DOCID'], row['D_CONTENT'])})
        | 'Trasfrom Result' >> beam.ParDo(convert_kbsta_formatted_map)
    )

    (
        processing_data
        #| 'Write Data' >> beam.io.WriteToText('extracted_')
        | 'Loading to Bigquery' >> beam.io.WriteToBigQuery(
            table=table+"_result",
            dataset=dataset,
            project=project_id,
            #schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            batch_size=int(100)
        )
    )

    #pipeline.run()
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    #print(fileread('keyword_bank_result.schema'))
    main(sys.argv)
