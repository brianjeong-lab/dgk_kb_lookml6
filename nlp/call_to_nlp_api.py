import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_messages
import sys
import json
import time

import googleapiclient.discovery

project_id = 'kb-daas-dev' # your project ID
dataset_id = 'master_200722' # your dataset ID
table_id = 'keyword_bank_nlp' # your table ID

# for Standalone Test
options = {
    'project': project_id,
    'runner:': 'DirectRunner'
    }

options = PipelineOptions(flags=[], **options)  # create and set your PipelineOptions

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

def get_native_encoding_type():
    """Returns the encoding type that matches Python's native strings."""
    if sys.maxunicode == 65535:
        return 'UTF16'
    else:
        return 'UTF32'


def analyze_entities(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding,
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeEntities(body=body)
    response = request.execute()

    return response

def analyze_sentiment(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeSentiment(body=body)
    response = request.execute()

    return response

# ai api 콜 함수 # content에 분석할 내용 담기
def call_nlp_api_entities(content):
    
    start = time.time()

    response = analyze_entities(content, get_native_encoding_type())
    #response["response"] = { "status_code" : response.status_code , "proc_time" : response.proc_time, "err_msg" : response.err_msg }
    response["proc_time"] = time.time() - start

    return response

# ai api 콜 함수 # content에 분석할 내용 담기
def call_nlp_api_sentiment(content):

    start = time.time()
    response = analyze_sentiment(content, get_native_encoding_type())
    #response["response"] = { "status_code" : response.status_code , "proc_time" : response.proc_time, "err_msg" : response.err_msg }

    response["proc_time"] = time.time() - start

    return response

# formated
def convert_formatted_map(row):

    entities = []
    sentiment = {"score": 0, "magnitude": 0}
    sentiments = []
    response = {"entities_proc_time": 0, "sentiment_proc_time": 0}

    row_entities = row["ENTITIES"]
    row_sentiment = row["SENTIMENT"]

    if row_entities is not None:
        response["entities_proc_time"] = row_entities["proc_time"]
        for entity in row_entities["entities"]:
            entities.append({"name": entity["name"], "type": entity["type"], "salience": entity["salience"]})

    if row_sentiment is not None:
        response["sentiment_proc_time"] = row_sentiment["proc_time"]
        sentiment = row_sentiment["documentSentiment"]

        for s in row_sentiment["sentences"]:
            sentiments.append({
                "text" : s["text"]["content"]
                , "magnitude": s["sentiment"]["magnitude"]
                , "score": s["sentiment"]["score"] 
            })

    return {
        'ID' : row['ID']
        , 'CRAWLSTAMP' : row['D_CRAWLSTAMP']
        , 'WRITESTAMP' : row['D_WRITESTAMP']
        , 'ENTITIES' : entities
        , 'SENTIMENT' : sentiment
        , 'SENTIMENTS' : sentiments
        , "RESPONSE" : response
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

def main1():

    #schema = fileread('keyword_bank_result.schema')
    #print("schema", schema)

    # for test
    pipeline = beam.Pipeline(options=options)

    raw_data = (pipeline
        | 'Read Data From BigQuery' >> beam.io.Read(
            beam.io.BigQuerySource(
                query="""
                    SELECT
                       ID
                       , D_CRAWLSTAMP
                       , D_WRITESTAMP
                       , D_CONTENT
                    FROM (
                      SELECT 
                           A.ID
                          , A.D_CRAWLSTAMP
                          , A.D_WRITESTAMP
                          , A.D_CONTENT 
                          , B.ID as BID
                      FROM 
                          (
                            SELECT 
                                *
                            FROM 
                                `kb-daas-dev.master_200722.keyword_bank` 
                            WHERE 
                                D_CRAWLSTAMP BETWEEN TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul') 
                                AND TIMESTAMP('2020-06-02 00:00:00', 'Asia/Seoul') ) A
                          LEFT OUTER JOIN (
                            SELECT 
                                ID 
                            FROM 
                                `kb-daas-dev.master_200722.keyword_bank_nlp` 
                            WHERE 
                                CRAWLSTAMP BETWEEN TIMESTAMP('2020-06-01 00:00:00', 'Asia/Seoul') 
                                AND TIMESTAMP('2020-06-02 00:00:00', 'Asia/Seoul') 
                          ) B
                          ON A.ID = B.ID
                    ) A
                    WHERE
                      BID is null
                """,
                project=project_id,
                use_standard_sql=True)
        )
    )

    processing_data = (raw_data
        #| 'Testing' >> beam.ParDo(DateExtractor())
        #| 'Testing' >> beam.Map(cleasing_contents)
        #| 'Testing 2' >> beam.GroupByKey()
        #| 'Testing 3' >> beam.Map(count_ones)
        | 'Cleansing' >> beam.Map(lambda row: {'ID':row['ID'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'D_CONTENT':cleasing_contents(row['D_CONTENT'])})
        | 'Call with KB STA API' >> beam.Map(lambda row: {'ID':row['ID'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'ENTITIES':call_nlp_api_entities(row['D_CONTENT']), 'SENTIMENT':call_nlp_api_sentiment(row['D_CONTENT'])})
        | 'Trasfrom Result' >> beam.Map(lambda row: convert_formatted_map(row))
    )

    (
        processing_data
        #| 'Write Data' >> beam.io.WriteToText('extracted_')
        | 'Loading to Bigquery' >> beam.io.WriteToBigQuery(
            table=table_id,
            dataset=dataset_id,
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
    main1()