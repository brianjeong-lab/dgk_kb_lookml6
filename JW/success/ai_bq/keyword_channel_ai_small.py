import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_messages
import requests
import csv
import sys
import json
import time

# import Python logging module.
import logging


# 불필요한 특수문자 제거 전처리 함수
def cleasing_contents(contents):
    remove_target_char = ["&", "="] # 본문에서 제거해야할 특수문자
    for char in remove_target_char:
        contents = contents.replace(char, " ")

    return contents

def cleansing_row(row):
    row["D_CONTENT"] = cleasing_contents(row["D_CONTENT"])
    return row

# ai api 콜 함수 # content에 분석할 내용 담기
def call_kbsta_api(content):
    # AWS
    # url = "http://3.34.18.1:8080/analyze"
    # GCP
    url = "http://35.241.3.78:8080/analyze"
    querystring = {"tasks":"d2c,kpe,kse"}
    body = "text=" + content
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

    except Exception as e:
        logging.error(str(e))
        response = {"text":'', "status_code":'999', "proc_time": time.time() - start, "err_msg": str(e)}

    return response


def call_kbsta_api_with_json(content):
    # JSON 디코딩
    try:
        response = call_kbsta_api(content)
        json_array = json.loads(response.text)
        json_array["response"] = { "status_code" : response.status_code , "proc_time" : response.proc_time, "err_msg" : response.err_msg }

        return json_array
    except Exception as e:
        logging.error(str(e))
        return None

# formated
def convert_kbsta_formatted_map(row):

    ss = []
    d2c = []
    kpe = []
    kse = []
    response = None

    if row['D_RESULT'] is not None:

        result = row['D_RESULT']

        if 'ss' in result.keys():
            ss = result['ss']

        if 'd2c' in result.keys():
            d2c = result['d2c']

        if 'kse' in result.keys():
            kse = result['kse']

        if 'kpe' in result.keys():
            kpe = result['kpe']

        if 'response' in result.keys():
            response = result['response']

    return { # 빅쿼리 기존 테이블 컬럼 추가시, 여기 추가하기 CHANNEL, S_NAME, SB_NAE
        'ID':row['ID']
        , 'CRAWLSTAMP':row['D_CRAWLSTAMP']
        , 'WRITESTAMP':row['D_WRITESTAMP']
        , 'CHANNEL':row['CHANNEL']
        , 'S_NAME':row['S_NAME']
        , 'SB_NAME':row['SB_NAME']
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

def main(pipeline_options, app_args):

    #schema = fileread('keyword_bank_result.schema')
    #print("schema", schema)

    pipeline = beam.Pipeline(options=pipeline_options)

    year = app_args.input_year
    month = app_args.input_month
    day = app_args.input_day
    hour = app_args.input_hour

    project = app_args.bq_project
    dataset = app_args.bq_dataset
    table = app_args.bq_table

    logging.info(f'[Start] Project : {project}')
    logging.info(f'base date : {year}-{month}-{day} {hour}:00:00')
    logging.info(f'dataset : {dataset}')
    logging.info(f'table : {table}')

    raw_data = (pipeline
        | '1.Read Data From BigQuery' >> beam.io.Read(
            beam.io.BigQuerySource(
                # 빅쿼리 기존 테이블 컬럼 추가시, 여기 추가하기 CHANNEL, S_NAME, SB_NAME
                query=f"""
                    SELECT
                        ID
                        , D_CRAWLSTAMP
                        , D_WRITESTAMP
                        , CHANNEL
                        , S_NAME
                        , SB_NAME
                        , D_CONTENT
                    FROM
                        `kb-daas-dev.master_200723.keyword_channel`
                """,
                project=project,
                use_standard_sql=True)
        )
    )


    processing_data = (raw_data
        | '2.Cleansing' >> beam.Map(lambda row: {'ID':row['ID'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'CHANNEL':row['CHANNEL'], 'S_NAME':row['S_NAME'], 'SB_NAME':row['SB_NAME'], 'D_CONTENT':cleasing_contents(row['D_CONTENT'])})
        | '3.Call with KB STA API' >> beam.Map(lambda row: {'ID':row['ID'], 'D_CRAWLSTAMP':row['D_CRAWLSTAMP'], 'D_WRITESTAMP':row['D_WRITESTAMP'], 'CHANNEL':row['CHANNEL'], 'S_NAME':row['S_NAME'], 'SB_NAME':row['SB_NAME'],'D_RESULT':call_kbsta_api_with_json(row['D_CONTENT'])})
        | '4.Trasfrom Result' >> beam.Map(lambda row: convert_kbsta_formatted_map(row))
    )

    (
        processing_data
        | '5.Loading to Bigquery' >> beam.io.WriteToBigQuery(
            table=f'{table}_result',
            dataset=dataset,
            project=project,
            #schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            batch_size=int(100)
        )
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--input.year", dest="input_year", required=True, help="Year for input data")
    parser.add_argument("--input.month", dest="input_month", required=True, help="Month for input data")
    parser.add_argument("--input.day", dest="input_day", required=True, help="Day for input data")
    parser.add_argument("--input.hour", dest="input_hour", required=True, help="Hour for input data")

    parser.add_argument("--bq.project", dest="bq_project", required=True, help="Project Name for Bigquery")
    parser.add_argument("--bq.dataset", dest="bq_dataset", required=True, help="Dataset Name for Bigquery")
    parser.add_argument("--bq.table", dest="bq_table", required=True, help="Table Name for Bigquery")

    app_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logging.getLogger().setLevel(logging.INFO)

    main(pipeline_options, app_args)
