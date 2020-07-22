import csv
import sys
import json
import time

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# import Python logging module.
import logging

proc_time = time.time()

table_info = [
      ["ID", "integer"]                #0
    , ["DOCID", "integer"]             #1
    , ["CHANNEL", "string"]
    , ["S_SEQ", "integer"]             #3
    , ["S_NAME", "string"]
    , ["SB_NAME", "string"]
    , ["D_CRAWLSTAMP", "timestamp"]    #6
    , ["D_TITLE", "string"]
    , ["D_URL", "string"]
    , ["D_CONTENT", "string"]
    , ["D_WRITER", "string"]
    , ["D_WRITESTAMP", "timestamp"]   #11
    , ["VIEW_COUNT", "integer"]       #12
    , ["REPLY_COUNT", "integer"]      #13
    , ["GOOD_COUNT", "integer"]       #14
    , ["BAD_COUNT", "integer"]        #15
    , ["JSON_DATA", "string"]
    , ["PROCSTAMP", "timestamp"]      #17
]

table_header = 'ID,DOCID,CHANNEL,S_SEQ,S_NAME,SB_NAME,D_CRAWLSTAMP,D_TITLE,D_URL,D_CONTENT,D_WRITER,D_WRITESTAMP,VIEW_COUNT,REPLY_COUNT,GOOD_COUNT,BAD_COUNT,JSON_DATA,PROCSTAMP'.split(',')
table_schema = json.load(open('./schema_0722.json', 'r'))

# avoiding error : field larger than field limit (131072)
csv.field_size_limit(sys.maxsize)

def cleansing(line):
    if line.count('\x00') > 0:
        return line.replace('\x00', '')
    else:
        return line

def csv_reader(line):
    csv.field_size_limit(sys.maxsize)
    return csv.reader([cleansing(line)])


def create_row(fields):
    featdict = {}
    for name, value in zip(table_header, fields):
        featdict[name] = value

    for i in [0, 1, 3, 6, 11, 12, 13, 14, 15]:
        try:
            featdict[table_info[i][0]] = int(featdict[table_info[i][0]])
        except:
            featdict[table_info[i][0]] = 0

    featdict["PROCSTAMP"] = proc_time # 새로 추가하는 컬럼(csv에 없는 컬럼이기 때문에 fields에 들어있지 않음)
    return featdict

def main(pipeline_args, app_args):
    pipeline = beam.Pipeline(options=pipeline_options)

    path = app_args.input_gcs_path
    project = app_args.bq_project
    dataset = app_args.bq_dataset
    table = app_args.bq_table

    logging.info(f'[Start] Project : {project}')
    logging.info(f'dataset : {dataset}')
    logging.info(f'table : {table}')
    logging.info(f'load file name : {path}')

    dataflow = (pipeline
                    | '1. Load Data' >> beam.io.ReadFromText(path)
                    | '2. CSV Parser' >> beam.Map(lambda line: next(csv_reader(line)))
                    | '3. Create Row' >> beam.Map(lambda fields: create_row(fields))
                    | '4. Loading to Bigquery' >> beam.io.WriteToBigQuery(
                                                    table=table,
                                                    dataset=dataset,
                                                    project=project,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                    batch_size=int(100)
                                                    )
                )

    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--input.gcs_path", dest="input_gcs_path", required=True, help="Input Rawdata Location (ex, gs://kb-daas-dev-raw-data/rsn/bank_zip/)")

    parser.add_argument("--bq.project", dest="bq_project", required=True, help="Project Name for Bigquery") # 구글클라우드 프로젝트 이름
    parser.add_argument("--bq.dataset", dest="bq_dataset", required=True, help="Dataset Name for Bigquery") # 적재할 데이터셋 이름
    parser.add_argument("--bq.table", dest="bq_table", required=True, help="Table Name for Bigquery") # 적재할 테이블 이름

    app_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logging.getLogger().setLevel(logging.INFO)

    main(pipeline_options, app_args)
