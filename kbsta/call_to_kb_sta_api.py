import apache_beam as beam
import csv
import sys

if __name__ == '__main__':

    # for test
    with beam.Pipeline('DirectRunner') as pipeline:

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
                    use_standard_sql=True)) 
            | 'Write Data' >> beam.io.WriteToText('extracted_')
        )

        pipeline.run()