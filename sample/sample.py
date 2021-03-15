import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import sys

def csv_reader(line):
    #if line.find('ID') >= 0:
        #return None

    return csv.reader([line])

def pprint(row):
    if row[0] == 'ID':
        return

    yield row

def main(month, day, hour):
        # for test
    pipeline = beam.Pipeline('DirectRunner')

    bankdatas = (pipeline
        #| 'Load Data' >> beam.io.ReadFromText(f'gs://kb-daas-dev-raw-data/rsn/bank_zip/{month}/{day}/{hour}.csv.gz')
        #| 'Load Data' >> beam.io.ReadFromText('gs://my_test_bk_0630/bank_data_0630/KBSTAR_0616_0630_01.csv.gz')
        | beam.Create([
          'ID,Name,Value',
          "1,TGKANG,TGKANG1",
          '2,SSS,SSS2',
          '3,TEST,TEST',
        ])
        | 'CSV Parser' >> beam.Map(lambda line: next(csv_reader(line)))
        | 'PRITN1' >> beam.ParDo(pprint)
        | 'PRITN2' >> beam.Map(lambda row: print("a", row))
    )

    # (bankdatas
    #     | 'create Row' >> beam.Map(lambda fields: create_row(fields))  
    #     | 'Loading to Bigquery' >> beam.io.WriteToBigQuery(
    #     table=table_id,
    #     dataset=dataset_id,
    #     project=project_id,
    #     schema=table_schema,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #     batch_size=int(100)
    #     )
    # )

    pipeline.run()

if __name__ == '__main__':
    month = 6
    day = 1
    #for hour in [3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]:
    #for hour in [11,12,13,14,15,16,17,18,19,20,21,22,23]:
    for hour in [11]:
        #print("hour {hour}")
        #print(f'Hour : {hour}')
        print(f'Running {month}/{day} {hour}:00')
        main(month, day, hour)

