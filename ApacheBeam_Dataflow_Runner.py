#------------Import Lib-----------------------#
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os, sys
from apache_beam.runners.dataflow.ptransform_overrides import CreatePTransformOverride
from apache_beam.runners.dataflow.ptransform_overrides import ReadPTransformOverride
import argparse
import logging
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime

#------------Set up BQ parameters-----------------------#
# Replace with Project Id
project = 'XXXXXXXXX'

#plitting Of Records----------------------#
class Transaction(beam.DoFn):
    def process(self, element):
        return [{"C1": element[1], "applianceName": element[2], "tenantName": element[3], "generateTime": element[4], "tenantId": element[5], "vsnId": element[6], "sentOctets": element[7], "recvdOctets": element[8], "sessCnt": element[9], "mstatsType": element[10], "duration": element[11], "siteName": element[12], "accCktName": element[13], "siteId": element[14], "accCktId": element[15]}]

class Transaction_rule(beam.DoFn):
    def process(self, element):
        return [{"C1": element[1], "applianceName": element[2], "tenantName": element[3], "generateTime": element[4], "tenantId": element[5], "vsnId": element[6], "sentOctets": element[7], "recvdOctets": element[8], "sessCnt": element[9], "mstatsType": element[10], "duration": element[11], "siteName": element[12], "accCktName": element[13], "ruleName": element[14]}]

class Transaction_local(beam.DoFn):
    def process(self, element):
        return [{"C1": element[1], "applianceName": element[2], "tenantName": element[3], "generateTime": element[4], "tenantId": element[5], "vsnId": element[6], "sentOctets": element[7], "recvdOctets": element[8], "sessCnt": element[9], "mstatsType": element[10], "duration": element[11], "localSiteName": element[12], "localAccCktName": element[13], "remoteSiteName": element[14], "remoteAccCktName": element[15], "ruleName": element[16]}]

class Transaction_Audit(beam.DoFn):
    def process(self, element):
        return [{"Run_ID": element[1], "Log_Type": element[2], "Data_Load_TS": element[3], "No_of_Records_Loaded": element[4], "Archive_File_Name": element[5]}]
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
          '--input',
          dest='input',
          help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)



    #data_f = sys.argv[1]
    logging.info('***********')
    logging.info(known_args.input)
    data_loading = (
        p1
        |'Read from File' >> beam.io.ReadFromText(known_args.input,skip_header_lines=0)
        |'Spliting of Fields' >> beam.Map(lambda record: record.split(','))
    )


    project_id = "XXXXXXXXXXX"
    dataset_id = 'Prod_Networking'
    table_schema = ('C1:STRING, applianceName:STRING, tenantName:STRING, generateTime:STRING, tenantId:STRING, vsnId:STRING, sentOctets:STRING, recvdOctets:STRING, sessCnt:STRING, mstatsType:STRING, duration:STRING, siteName:STRING, accCktName:STRING, siteId:STRING, accCktId:STRING')
    table_schema_rule = ('C1:STRING, applianceName:STRING, tenantName:STRING, generateTime:STRING, tenantId:STRING, vsnId:STRING, sentOctets:STRING, recvdOctets:STRING, sessCnt:STRING, mstatsType:STRING, duration:STRING, siteName:STRING, accCktName:STRING, ruleName:STRING')
    table_schema_local = ('C1:STRING, applianceName:STRING, tenantName:STRING, generateTime:STRING, tenantId:STRING, vsnId:STRING, sentOctets:STRING, recvdOctets:STRING, sessCnt:STRING, mstatsType:STRING, duration:STRING, localSiteName:STRING, localAccCktName:STRING, remoteSiteName:STRING, remoteAccCktName:STRING, ruleName:STRING')
    table_schema_Audit = ('Run_ID:INTEGER, Log_Type:STRING, Data_Load_TS:STRING, No_of_Records_Loaded:INTEGER, Archive_File_Name:STRING')



        # Persist to BigQuery
        # WriteToBigQuery accepts the data as list of JSON objects

#---------------------Type = bwMonLog----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Filter-bwMonLog' >> beam.Filter(lambda record: record[0] == 'bwMonLog')
        | 'Clean-bwMonLog' >> beam.ParDo(Transaction())
        | 'Write-bwMonLog' >> beam.io.WriteToBigQuery(
                                                    table='bwMonLog',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#---------------------Type = rule----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Filter-rule' >> beam.Filter(lambda record: record[0] == 'rule')
        | 'Clean-rule' >> beam.ParDo(Transaction_rule())
        | 'Write-rule' >> beam.io.WriteToBigQuery(
                                                    table='bwMonLog_rule',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema_rule,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#---------------------Type = local----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Filter-local' >> beam.Filter(lambda record: record[0] == 'local')
        | 'Clean-local' >> beam.ParDo(Transaction_local())
        | 'Write-local' >> beam.io.WriteToBigQuery(
                                                    table='bwMonLog_local',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema_local,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#---------------------Type = audit----------------------------------------------------------------------------------------------------------------------
    result = (
    data_loading
        | 'Filter-Audit' >> beam.Filter(lambda record: record[0] == 'audit')
        | 'Clean-Audit' >> beam.ParDo(Transaction_Audit())
        | 'Write-Audit' >> beam.io.WriteToBigQuery(
                                                    table='Audit_Network',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema_Audit,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))



    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  path_service_account = '/home/vi/AAAAAA.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()

