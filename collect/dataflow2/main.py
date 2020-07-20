import csv
import sys

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# import Python logging module.
import logging

table_info = [
    ["ID", "integer"]                  #0
    , ["DOCID", "integer"]             #1
    , ["CHANNEL", "string"]
    , ["S_SEQ", "integer"]             #3
    , ["S_NAME", "string"]
    , ["SB_NAME", "string"]
    , ["D_CRAWLSTAMP", "integer"]      #6
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
]

table_header = 'ID,DOCID,CHANNEL,S_SEQ,S_NAME,SB_NAME,D_CRAWLSTAMP,D_TITLE,D_URL,D_CONTENT,D_WRITER,D_WRITESTAMP,VIEW_COUNT,REPLY_COUNT,GOOD_COUNT,BAD_COUNT,JSON_DATA'.split(',')
table_schema = 'ID:integer,DOCID:integer,CHANNEL:string,S_SEQ:integer,S_NAME:string,SB_NAME:string,D_CRAWLSTAMP:timestamp,D_TITLE:string,D_URL:string,D_CONTENT:string,D_WRITER:string,D_WRITESTAMP:timestamp,VIEW_COUNT:integer,REPLY_COUNT:integer,GOOD_COUNT:integer,BAD_COUNT:integer,JSON_DATA:string'

# avoiding error : field larger than field limit (131072)
csv.field_size_limit(1000000)

def cleansing(line):
    if line.count('\x00') > 0:
        return line.replace('\x00', '')
    else:
        return line

def csv_reader(line):
    csv.field_size_limit(1000000)
    return csv.reader([cleansing(line)])

def csv_reader2(line):
    try:
        row = csv.reader([line])
        listed_row = list(row)
    
        return csv.reader([line])
    # avoiding _csv.Error: line contains NULL byte
    except Exception as e: 
        #print('Null Data')
        #line_new = line.replace('\x00', '')
        #row = csv.reader([line_new])
        print('################# ERROR ####################')
        logging.error(str(e))

    return csv.reader([line.replace('\x00', '')])

def create_row(fields):
    featdict = {}
    for name, value in zip(table_header, fields):
        featdict[name] = value

    for i in [0, 1, 3, 6, 11, 12, 13, 14, 15]:
        try:
            featdict[table_info[i][0]] = int(featdict[table_info[i][0]])
        except:
            featdict[table_info[i][0]] = 0

    return featdict

def main(pipeline_args, app_args):
    # for test
    # Removed because of Double running
    # with beam.Pipeline(options=pipeline_options) as pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

    path = app_args.input_path
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

    bankdatas = (pipeline
        #| 'Load Data' >> beam.io.ReadFromText(f'{path}/{month}/{day}/{hour}.csv.gz')
        | 'Load Data' >> beam.io.ReadFromText('gs://my_test_bk_0630/bank_data_0630/KBSTAR1_0616_0630_02.csv.gz')
        | 'CSV Parser' >> beam.Map(lambda line: next(csv_reader(line)))
    )

    (bankdatas
        | 'create Row' >> beam.Map(lambda fields: create_row(fields))  
        | 'Loading to Bigquery' >> beam.io.WriteToBigQuery(
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
    parser.add_argument("--input.path", dest="input_path", required=True, help="Input Rawdata Location (ex, gs://kb-daas-dev-raw-data/rsn/bank_zip/)")
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

    # month = 6
    # day = 1
    # #for hour in [3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]:
    # for hour in [11,12,13,14,15,16,17,18,19,20,21,22,23]:
    #     #print("hour {hour}")
    #     #print(f'Hour : {hour}')
    #     print(f'Running {month}/{day} {hour}:00')
       

