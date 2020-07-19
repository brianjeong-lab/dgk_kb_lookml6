import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_messages
import requests
import csv
import sys

project_id = 'kb-daas-dev' # your project ID

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
    return contents

def cleansing_row(row):
    row["D_CONTENT"] = cleasing_contents(row["D_CONTENT"])
    return row

# ai api 콜 함수 # content에 분석할 내용 담기
def call_kbsta_api(content):
    url = "http://3.34.18.1:8080/analyze"
    querystring = {"tasks":"d2c,kpe,kse"}
    body = "text=" + content
    body = body.encode(encoding='utf-8')
    headers = {
        'apikey': "5rbeC7bMzbynvbcNqGwOnp5Tll2PUB9B",
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
        }
    response = requests.request("POST", url, data=body, headers=headers, params=querystring)
    return response.text

if __name__ == '__main__':

    # for test
    with beam.Pipeline(options=options) as pipeline:

        raw_data = (pipeline
            | 'Read Data From BigQuery' >> beam.io.Read(
                beam.io.BigQuerySource(
                    query="""
                        SELECT 
                            ID
                            , D_WRITESTAMP
                            , D_CONTENT 
                        FROM 
                            `kb-daas-dev.raw_data.rsn_bank` 
                        WHERE 
                            DATE(D_WRITESTAMP) = "2020-05-30" 
                        LIMIT 1000
                    """,
                    project=project_id,
                    use_standard_sql=True))
            | 'CSV Parser' >> beam.Map(lambda row: next(cleasing_row(row)))
            | 'Write Data' >> beam.io.WriteToText('extracted_')
        )

        pipeline.run()
