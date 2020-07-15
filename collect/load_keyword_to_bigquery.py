import apache_beam as beam
import csv

if __name__ == '__main__':

    # for test
    with beam.Pipeline('DirectRunner') as pipeline:

      bankdatas = (pipeline
         | beam.io.ReadFromText('gs://kb-daas-dev-raw-data/rsn/bank_zip/6/1/0.csv.gz')
         | beam.Map(lambda line: next(csv.reader([line])))
         | beam.Map(lambda fields: (fields[0], fields[1], fields[2]))
      )

      (bankdatas 
         | beam.Map(lambda bankdatas_data: '{},{},{}'.format(bankdatas_data[0], bankdatas_data[1], bankdatas_data[2]) )
         | beam.io.WriteToText('extracted_bank')
      )

      pipeline.run()