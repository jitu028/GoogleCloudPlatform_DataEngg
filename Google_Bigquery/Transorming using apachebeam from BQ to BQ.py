#This script will read data from bigquery make transformation using apache beam and write it back to bigquery table

#import libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io.gcp.internal.clients import bigquery as bq

#instating a pipeline
options = PipelineOptions()
p4 = beam.Pipeline(options=options)

#query3 = "SELECT * FROM `dca-sandbox-project-4.00_bigquery_00.AB_table'"
#reading data from bigquery table in the form of query
pc_split = (
    p4
    | beam.io.ReadFromBigQuery(query='select * from [dca-sandbox-project-4.00_bigquery_00.AB_table]')
)

#function to convert data into json structure
def to_json(Row):
	
	json_str = {"Day":Row["Day"],
                "Conversion_A": Row["Conversion_A"],  #Conversion_A
                "Conversion_B":Row["Conversion_B"]
                 }

	return json_str

#Schema and output table name (It will Config driven)	
table_schema = 'Day:INTEGER,Conversion_A:FLOAT,Conversion_B:FLOAT'
output_pattern = 'dca-sandbox-project-4:00_bigquery_00.transformed_dummy_data021'


#After converting to json writing back to Bigquery
(pc_split
|	"convert to json" >> beam.Map(to_json)
|	"write to bq" >> beam.io.WriteToBigQuery(
output_pattern,
schema = table_schema,
create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
)
)

#runner instance
from apache_beam.runners.runner import PipelineState
ret = p4.run()
if ret.state == PipelineState.DONE:
	print('Suceful run')
else:
	print('Error while running pipeline')


#python bq_bq_transform.py --runner direct --project dca-sandbox-project-4 --temp_location gs://c360_apachebeam_01/tmp --region us-west1