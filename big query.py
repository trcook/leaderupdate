import httplib2
import apiclient
from apiclient import discovery
import webbrowser
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
import os
import re
import pandas as pd

"""
This environment var must be set for this to work. Environment variable must point to api key json file.
api key json file is generated at
https://console.developers.google.com/apis/credentials
Under SERVICE ACCOUNT
This is disceptive since we want a key to access the api. Using a service account key
allows us to avoid oauth authorization

"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/tom/.client_secret.json'

credentials = GoogleCredentials.get_application_default()


"""
This builds the service. The service is what is used to call the big query
api
"""

service = build('bigquery', 'v2', credentials=credentials)


"""
get projects
"""
service.projects().list().execute()

"""
get and hold project id -- needed for querying
"""
proj_id = service.projects().list().execute()['projects'][0]['id']
gdelt_project_id = "gdelt-bq"


"""
Get datasets in a project:
"""
x = service.datasets()
gdelt_datasets = x.list(projectId=gdelt_project_id).execute()

gdelt_datasets

"""
We want the gdelt-full dataset:
"""

gdelt_full_id = [i['id'] for i in gdelt_datasets['datasets'] if re.match('.*full.*',i['id']) ][0]

gdelt_full_id_dataset_only = [i['datasetReference']['datasetId'] for i in gdelt_datasets['datasets'] if re.match('.*full.*',i['id']) ][0]

"""
Get tables in dataset
"""

gdelt_full_tables=service.tables()
gdelt_full_tables=gdelt_full_tables.list(                                datasetId=gdelt_full_id_dataset_only,                                projectId= gdelt_project_id).execute()

"""
Get just the table for events
"""

gdelt_full_events_table = [i for i in gdelt_full_tables['tables']  if re.match(".*events.*",i['id'])][0]["id"]

jerbs = service.jobs()
"""
Must specify your own project in query
This is because the project is used for billing. not for looking up tables
We specify the project, dataset, and tables in the sql of the query itself
"""

sql_query = '''
    SELECT
         MonthYear,
         IFNULL(Actor1CountryCode,Actor2CountryCode) Country,eventcode,
         COUNT(eventcode) as count
    FROM
         [%s]
    Where
         eventcode IN (
         "1441","0241","0244","0341","0831","0834","1231","1234"
             )


    GROUP BY
         MonthYear,country,eventcode
    HAVING
         NOT Country = 'null'
         AND
         NOT Country == "None"
        '''% gdelt_full_events_table

get_years = jerbs.query(projectId=proj_id,body={
         "query":sql_query
     })
x = get_years.execute()
x_rows = [i['name'] for i in x['schema']['fields']]

m = pd.DataFrame([[j['v'] for j in i['f']] for i in x['rows'] ])
m.columns = x_rows
m = pd.DataFrame([[j['v'] for j in i['f']] for i in x['rows'] ])
m.columns = x_rows
m['year'] = [int(str(i)[0:4]) for i in m.MonthYear]
m['month'] = [int(str(i)[4:7]) for i in m.MonthYear]


m.loc[:,['Country','year','month','eventcode','count']]


codes = {"1441": "Obstruct passage to demand leadership change",
"0241": "Appeal for Leadership Change",
"0244": "Appeal for change in institutions regime",
"0341": "express intent to change leadership",
"0831": "Accede to demands for change in leadership",
"0834": "Accede to demands for change in institutions, regime",
"1231": "Reject request to change leadership",
"1234": "Reject request for change in institutions, regime"}




for i,v in codes.items():
    m.loc[m['eventcode'].isin([i]),'description'] =v
