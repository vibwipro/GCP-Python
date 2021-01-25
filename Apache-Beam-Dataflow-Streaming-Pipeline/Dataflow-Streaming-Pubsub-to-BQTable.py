
#------------Import Lib-----------------------#
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os, sys, time
import argparse
import logging
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime

#------------Set up BQ parameters-----------------------#
# Replace with Project Id
project = 'xxxxxxxxxxx'
Pubsub_subscription='projects/xxxxxxxxxxx/subscriptions/Pubsubdemo_subscription'
#plitting Of Records----------------------#

class Transaction_ECOM(beam.DoFn):
    def process(self, element):
        logging.info(element)

        result = json.loads(element)
        data_bkt = result.get('_bkt','null')
        data_cd=result.get('_cd','null')
        data_indextime=result.get('_indextime','0')
        data_kv=result.get('_kv','null')
        data_raw=result['_raw']
        data_raw1=data_raw.replace("\n", "")
        data_serial=result.get('_serial','null')
        data_si = str(result.get('_si','null'))
        data_sourcetype =result.get('_sourcetype','null')
        data_subsecond = result.get('_subsecond','null')
        data_time=result.get('_time','null')
        data_host=result.get('host','null')
        data_index=result.get('index','null')
        data_linecount=result.get('linecount','null')
        data_source=result.get('source','null')
        data_sourcetype1=result.get('sourcetype','null')
        data_splunk_server=result.get('splunk_server','null')

        return [{"datetime_indextime": time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(int(data_indextime))), "_bkt": data_bkt, "_cd": data_cd,  "_indextime": data_indextime,  "_kv": data_kv,  "_raw": data_raw1,  "_serial": data_serial,  "_si": data_si, "_sourcetype": data_sourcetype, "_subsecond": data_subsecond, "_time": data_time, "host": data_host, "index": data_index, "linecount": data_linecount, "source": data_source, "sourcetype": data_sourcetype1, "splunk_server": data_splunk_server}]



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)



    data_loading = (
        p1
        | "Read Pub/Sub Messages" >> beam.io.ReadFromPubSub(subscription=Pubsub_subscription)

    )


    project_id = "xxxxxxxxxxx"
    dataset_id = 'test123'
    table_schema_ECOM = ('datetime_indextime:DATETIME, _bkt:STRING, _cd:STRING, _indextime:STRING, _kv:STRING, _raw:STRING, _serial:STRING, _si:STRING, _sourcetype:STRING, _subsecond:STRING, _time:STRING, host:STRING, index:STRING, linecount:STRING, source:STRING, sourcetype:STRING, splunk_server:STRING')

        # Persist to BigQuery
        # WriteToBigQuery accepts the data as list of JSON objects

#---------------------Index = ITF----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Clean-ITF' >> beam.ParDo(Transaction_ECOM())
        | 'Write-ITF' >> beam.io.WriteToBigQuery(
                                                    table='CFF_ABC',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema_ECOM,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))

    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  path_service_account = '/home/vibhg/Splunk/CFF/xxxxxxxxxxx-abcder125.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()
