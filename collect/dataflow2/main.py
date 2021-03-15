import csv
import sys
import time

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
    , ["D_CRAWLSTAMP", "integer"]      #3
    , ["S_SEQ", "integer"]             #4
    , ["S_NAME", "string"]
    , ["SB_NAME", "string"]
    , ["D_TITLE", "string"]
    , ["D_URL", "string"]
    , ["D_CONTENT", "string"]
    , ["D_WRITER", "string"]
    , ["D_WRITER_ID", "string"]
    , ["D_WRITESTAMP", "timestamp"]   #12
    #, ["VIEW_COUNT", "integer"]       #13
    #, ["REPLY_COUNT", "integer"]      #14
    #, ["GOOD_COUNT", "integer"]       #15
    #, ["BAD_COUNT", "integer"]        #16
    , ["JSON_DATA", "string"]
    , ["PROCSTAMP", "integer"]
]


#"ID","DOCID","CHANNEL","S_SEQ","S_NAME","SB_NAME","D_CRAWLSTAMP","D_TITLE","D_URL","D_CONTENT","D_WRITER","D_WRITER_ID","D_WRITESTAMP","JSON_DATA"

table_header = 'ID,DOCID,CHANNEL,D_CRAWLSTAMP,S_SEQ,S_NAME,SB_NAME,D_TITLE,D_URL,D_CONTENT,D_WRITER,D_WRITER_ID,D_WRITESTAMP,JSON_DATA,PROCSTAMP'.split(',')
table_schema = 'ID:integer,DOCID:integer,CHANNEL:string,D_CRAWLSTAMP:timestamp,S_SEQ:integer,S_NAME:string,SB_NAME:string,D_TITLE:string,D_URL:string,D_CONTENT:string,D_WRITER:string,D_WRITER_ID:string,D_WRITESTAMP:timestamp,JSON_DATA:string,PROCSTAMP:timestamp'

# avoiding error : field larger than field limit (131072)
csv.field_size_limit(sys.maxsize)
curtimestamp = time.time()

def cleansing(line):
    if line.count('\x00') > 0:
        return line.replace('\x00', '')
    else:
        return line

def csv_reader(line):
    csv.field_size_limit(sys.maxsize)
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

    for i in [0, 1, 3, 4, 12]:
        try:
            featdict[table_info[i][0]] = int(featdict[table_info[i][0]])
        except:
            logging.warn("Invalid {}'s Int value : {}={}".format(featdict["ID"], table_info[i][0], featdict[table_info[i][0]]))

            # ID or DOCID is null
            if i == 0 or i == 1:
                # ignore it
                logging.warn("Skip {}'s DATA {} {}".format(featdict["ID"], featdict["DOCID"], featdict["CHANNEL"]))
                return

            featdict[table_info[i][0]] = 0

    featdict["PROCSTAMP"] = curtimestamp

    # Validation 
    # D_CRAWLTIMESTAMP
    if featdict["D_CRAWLSTAMP"] > curtimestamp or featdict["D_CRAWLSTAMP"] < curtimestamp - 86400*365:
        logging.warn("Invalid {}'s D_CRAWLSTAMP : {}".format(featdict["ID"], featdict["D_CRAWLSTAMP"]))
        featdict["D_CRAWLSTAMP"] = featdict["D_WRITESTAMP"]
        
    # D_WRITESTAMP
    if featdict["D_WRITESTAMP"] > curtimestamp or featdict["D_WRITESTAMP"] < curtimestamp - 86400*365:
        logging.warn("Invalid  {}'s D_WRITESTAMP : {}".format(featdict["ID"], featdict["D_WRITESTAMP"]))

        if featdict["D_CRAWLSTAMP"] > curtimestamp or featdict["D_CRAWLSTAMP"] < curtimestamp - 86400*365:
            featdict["D_WRITESTAMP"] = featdict["D_CRAWLSTAMP"] = curtimestamp
        else:
            featdict["D_WRITESTAMP"] = featdict["D_CRAWLSTAMP"]

    yield featdict

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
        #| '1.Load Data' >> beam.io.ReadFromText(f'gs://kb-daas-dev-raw-data/new_rsn/bank/KBSTAR_은행키워드데이터_0601_0630_{hour}.csv')
        | '1.Load Data' >> beam.io.ReadFromText(f'gs://kb-daas-dev-raw-data/new_rsn/corona/KBSTAR_코로나키워드데이터_0616_0630_{hour}.csv')
        | '2.CSV Parser' >> beam.Map(lambda line: next(csv_reader(line)))
    )

    (bankdatas
        | '3.Create Row' >> beam.ParDo(create_row)  
        | '4.Loading to Bigquery' >> beam.io.WriteToBigQuery(
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
       

