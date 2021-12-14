# pytype: skip-file

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def main(argv=None, save_main_session=True):

    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      # CHANGE 1/6: The Google Cloud Storage path is required
      # for outputting the results.
      default='gs://c360_apachebeam_01/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=dca-sandbox-project-4',
        '--job = dataflowjob01',
        '--region=us-central1',
        '--staging_location=gs://c360_apachebeam_01/staging',
        '--temp_location=gs://c360_apachebeam_01/AND_TEMP_DIRECTORY',
        '--template_location = gs://c360_apachebeam_01/templates0011/Food'
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
    pipeline_option = PipelineOptions(pipeline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = save_main_session

    def remove_special_characters(row):
        cols = row.split(',')
        ret = ""
        for col in cols:
            clean_col = re.sub(r'[?%&]','', col)
            ret = ret + clean_col + ','
            ret = ret[:-1]
        return ret


    with beam.Pipeline(options=pipeline_option) as p:
        # Read the text file[pattern] into a PCollection.
        lines = (
            p 
            | beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | beam.Map(lambda row: row.lower())
            | beam.Map(remove_special_characters)
            | beam.Map(lambda row: row+',1'))    

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        lines |"Write to GCS" >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()

#https://cloud.google.com/dataflow/docs/guides/using-command-line-intf
# python 0food.py --job_name dataflow --runner DataflowRunner --project dca-sandbox-project-4  --staging_location gs://c360_apachebeam_01/staging --temp_location gs://c360_apachebeam_01/AND_TEMP_DIRECTORY --template_location gs://c360_apachebeam_01/templates0011/Food  --region us-central1
#https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run
# gcloud dataflow jobs run dataflow --gcs-location="gs://c360_apachebeam_01/templates0011/temp"
