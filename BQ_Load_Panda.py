import pandas as pd
from pandas.io import gbq
import os
dept_dt=pd.read_csv('dept_data')
#print(dept_dt)

# Replace with Project Id
project = 'XXXX'

#Replace with service account path
path_service_account = 'AAAAA.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account


dept_dt.to_gbq(destination_table='test1.Emp_data1',project_id='XXXX',if_exists='fail')