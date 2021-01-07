import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    source_config = relational_db.SourceConfiguration(
        drivername='postgresql+pg8000',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
    )
    records = p | "Reading records from db" >> relational_db.Read(
        source_config=source_config,
        table_name='months',
    )
    records | 'Writing to stdout' >> beam.Map(print)