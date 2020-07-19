import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_messages
import csv
import sys

project_id = 'kb-daas-dev' # your project ID

# for Standalone Test
options = {
    'project': project_id,
    'runner:': 'DirectRunner'
    }

options = PipelineOptions(flags=[], **options)  # create and set your PipelineOptions

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
            | 'Write Data' >> beam.io.WriteToText('extracted_')
        )

        pipeline.run()
