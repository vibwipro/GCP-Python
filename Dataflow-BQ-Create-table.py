from dateutil.parser import parse
import datetime
from google.cloud import bigquery


stream_query = """create table `project.dataset.YOUR_NEW_TABLE4`  partition by date(_time) as SELECT * FROM `project.dataset.YOUR_NEW_TABLE1` WHERE 1=2"""

stream_client = bigquery.Client()

stream_Q = stream_client.query(stream_query)
stream_data_df = stream_Q.to_dataframe()
