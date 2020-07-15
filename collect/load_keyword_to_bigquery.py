import apache_beam as beam
import csv
import sys

project_id = 'kb-daas-dev' # your project ID
dataset_id = 'raw_data' # your dataset ID
table_id = 'keyword_bank' # your table ID

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
csv.field_size_limit(sys.maxsize)

def cleansing(line):
    if line.count('\x00') > 0:
        return line.replace('\x00', '')
    else:
        return line

def csv_reader(line):

    return csv.reader([cleansing(line)])

def csv_reader2(line):
    try:
        row = csv.reader([line])
        listed_row = list(row)
    
        return csv.reader([line])
    # avoiding _csv.Error: line contains NULL byte
    except: 
        #print('Null Data')
        #line_new = line.replace('\x00', '')
        #row = csv.reader([line_new])
        print('################# ERROR ####################')

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

if __name__ == '__main__':

    # for test
    with beam.Pipeline('DirectRunner') as pipeline:

        bankdatas = (pipeline
            | 'Load Data' >> beam.io.ReadFromText('gs://kb-daas-dev-raw-data/rsn/bank_zip/6/1/0.csv.gz')
            #| 'Load Data' >> beam.io.ReadFromText('gs://my_test_bk_0630/bank_data_0630/KBSTAR_0616_0630_01.csv.gz')
            | 'CSV Parser' >> beam.Map(lambda line: next(csv_reader(line)))
        )

        # (bankdatas 
        #     | beam.Map(lambda bankdatas_data: '{}'.format(csv.writer) )
        #     | beam.io.WriteToText('extracted_bank')
        # )
        
        # (bankdatas
        #   | 'create Row' >> beam.Map(lambda fields: create_row(fields)) 
        #   | beam.io.WriteToText('extracted_bank')
        # )

        (bankdatas
          | 'create Row' >> beam.Map(lambda fields: create_row(fields))  
          | 'Loading to Bigquery' >> beam.io.WriteToBigQuery(
            table=table_id,
            dataset=dataset_id,
            project=project_id,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            batch_size=int(100)
          )
        )

        pipeline.run()