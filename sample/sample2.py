from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window

from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions



def print_row(row):
    print(row)
    print(type(row))

def filter_out_nones(row):
  if row is not None:
    yield row
#   else:
#     print('we found a none! get it out')


def run(argv=None):
    pipeline_options = PipelineOptions()
    #pipeline_options.view_as(SetupOptions).save_main_session = True
    #pipeline_options.view_as(StandardOptions).streaming = True


    p = beam.Pipeline(options=pipeline_options)



    data = ['test1 message','test2 message',None,'test3 please work']

## this does seem to return only the values I would hope for based on the console log

    testlogOnly = (p | "makeData" >> beam.Create(data)
               | "filter" >> beam.ParDo(filter_out_nones)
               | "printtesting" >> beam.Map(print_row))
            #  | 'encoding' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
            #  | "writing" >> beam.io.WriteToPubSub("projects/??/topics/??"))


##    testlogAndWrite = (p | "MakeWriteData" >> beam.Create(data)
            #  | "filterHere" >> beam.ParDo(filter_out_nones)
            #   | "printHere" >> beam.Map(print_row)
## below here does not work due to the following message
## AttributeError: 'NoneType' object has no attribute 'encode' [while running 'encodeHere']
            #   | 'encodeHere' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
            # | "writeTest" >> beam.io.WriteToPubSub("projects/??/topics/??"))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()