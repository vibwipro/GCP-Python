import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os

# Replace with Project Id
project = 'XXXXXXXXX'

#Replace with service account path
path_service_account = 'AAAAAAAAA.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

option = PipelineOptions()
#option.view_as(StandardOptions).streaming = True
#option.view_as(SetupOptions).save_main_session = True


class Transaction(beam.DoFn):
    def process(self, element):
        #An alternative way to split elements
        #C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15 = element.split(',')
        #return [{"C1": C1, "C2": C2, "C3": C3, "C4": C4, "C5": C5, "C6": C6, "C7": C7, "C8": C8, "C9": C9, "C10": C10, "C11": C11, "C12": C12, "C13": C13, "C14": C14, "C15": C15}]
        return [{"C1": element[0], "C2": element[1], "C3": element[2], "C4": element[3], "C5": element[4], "C6": element[5], "C7": element[6], "C8": element[7], "C9": element[8], "C10": element[9], "C11": element[10], "C12": element[11], "C13": element[12], "C14": element[13]}]


class FilterRec(beam.DoFn):
  def process(self, element):
    if element[13][:10] != ' ruleName=':
      return [element]

p1 =beam.Pipeline()

data_f = 'network.txt'

attendance_count = (
    p1
    |'Read from File' >> beam.io.ReadFromText(data_f,skip_header_lines=0)
    #Records filtering way
    |beam.Filter( lambda row: row.split(',')[0].split(' ')[4] == 'bwMonLog')
    |beam.Map(lambda record: record.split(','))
    |beam.ParDo(FilterRec())
    #An other way to filter data
    #|beam.Filter( lambda row: SUBSTR(row.split(',')[13], 1, 9) != ' ruleName')
    | 'Clean the items' >> beam.ParDo(Transaction())
)


project_id = "XXXXXXX"
dataset_id = 'test1'
table_id = 'bwMonLog'
table_schema = ('C1:STRING, C2:STRING, C3:STRING, C4:STRING, C5:STRING, C6:STRING, C7:STRING, C8:STRING, C9:STRING, C10:STRING, C11:STRING, C12:STRING, C13:STRING, C14:STRING, C15:STRING')


    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects

attendance_count | 'Write' >> beam.io.WriteToBigQuery(
                                                table=table_id,
                                                dataset=dataset_id,
                                                project=project_id,
                                                schema=table_schema,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                batch_size=int(100)
                                                )


result = p1.run()
result.wait_until_finish()
