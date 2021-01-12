import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    source_config = relational_db.SourceConfiguration(
        drivername='postgresql+pg8000',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
        create_if_missing=True,
    )
    table_config = relational_db.TableConfiguration(
        name='months',
        create_if_missing=True
    )
    months | 'Writing to DB' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    )