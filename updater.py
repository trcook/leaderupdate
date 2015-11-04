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
import sys
import time

"""
setup service stuff
"""

def make_service(x='bigquery'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/tom/.client_secret.json'
    credentials = GoogleCredentials.get_application_default()
    service = build(x, 'v2', credentials=credentials)
    return service
service = make_service()

class WORKING_PROJ(object):
    def __init__(self,bqservice,query_project = "gdelt-bq",proj_id = None):
        if not proj_id:
            self.proj_id =\
                bqservice.projects().list().execute()['projects'][0]['id']
        self.service = bqservice
        self.query_project = query_project
        self.tables = []
        self.get_datasets()
        self.get_tables()
        self.format_tabframe()
    def get_datasets(self):
        dsr = self.datasets_raw = self.service.datasets().list(\
        projectId = self.query_project).execute()
        self.datasets =[i['datasetReference']['datasetId'] for i in dsr['datasets']]
    def get_tables(self):
        for ds in self.datasets:
            tabr = self.service.tables().list(\
            projectId = self.query_project,datasetId = ds\
            ).execute()
            tabs = tabr['tables']
            for table in tabs:
                self.tables.append(table)
    def format_tabframe(self):
        tabframe = self.table_frame = pd.DataFrame(self.tables)
        tabframe['tableId'] = tabframe.tableReference.map(lambda i:i['tableId'])
        tabframe['projectId'] =\
                            tabframe.tableReference.map(lambda i:i['projectId'])
        tabframe['datasetId'] =\
                            tabframe.tableReference.map(lambda i:i['datasetId'])
        self.table_frame = tabframe = tabframe.loc[:,['projectId','datasetId','tableId']]
    def tab_desc(self):
        self.table_frame['desc'] = self.table_frame.apply(lambda\
        i:self.service.tables().get(projectId = i['projectId'],\
        datasetId=i['datasetId'],tableId=i['tableId']).execute(),axis=1)
        self.table_frame['vars'] = self.table_frame['desc'].map(lambda i: [j['name'] for j in i['schema']['fields']])
        self.table_frame['locator'] = self.table_frame.loc[:,\
                                            'desc'].apply(lambda\
                                            i:i['id'])



wp = WORKING_PROJ(service)
wp.tab_desc()
wp.table_frame.loc[wp.table_frame['datasetId']=='full','locator'][3]

wp.table_frame.loc['desc'].apply(lambda i:i['id'])

#
# x = service.datasets()
# gdelt_datasets = x.list(projectId=gdelt_project_id).execute()
# gdelt_full_id = [i['id'] for i in gdelt_datasets['datasets'] if\
#         re.match('.*full.*',i['id']) ][0]
# gdelt_full_id_dataset_only = [i['datasetReference']['datasetId']\
#         for i in gdelt_datasets['datasets'] if re.match('.*full.*',i['id']) ][0]
#
# gdelt_full_tables=service.tables()
# gdelt_full_tables=gdelt_full_tables.list(datasetId=gdelt_full_id_dataset_only,                                projectId= gdelt_project_id).execute()
#
# gdelt_full_events_table = [i for i in gdelt_full_tables['tables']  if re.match(".*events.*",i['id'])][0]["id"]



"""
jobs object
"""
jerbs = service.jobs()


"""
Cameo codes for searching
"""



"""
import leaders
"""

os.getcwd()
dat = pd.read_csv("./current_office.csv")

dat['LEADER'] = [i.split(' ')[-1].upper() for i in dat.actor1name]



codes = {"1441": "Obstruct passage to demand leadership change",
"0241": "Appeal for Leadership Change",
"0244": "Appeal for change in institutions regime",
"0341": "express intent to change leadership",
"0831": "Accede to demands for change in leadership",
"0834": "Accede to demands for change in institutions, regime",
"1231": "Reject request to change leadership",
"1234": "Reject request for change in institutions, regime"}


def stringer(x=list()):
    return ', '.join('"{0}"'.format(w) for w in x)

codestr = stringer(codes.keys())
codestr

codes.keys()
myList = ['a','b','c','d']


base_query = '''
    SELECT
        MonthYear,
        IFNULL(Actor1CountryCode,Actor2CountryCode) Country,
        eventcode,
        Actor1name,
        COUNT(eventcode) as count,
        min(SOURCEURL)
    FROM
        [%s]
    Where
        eventcode IN (
        %s
        )
        AND
        MonthYear==201510
        AND Actor1name CONTAINS '%s'
    GROUP BY
         MonthYear,country,eventcode,Actor1name
    HAVING
    Country =="%s"
    AND
         NOT Country = 'null'
    AND
         NOT Country == "None"
        '''



wp.loc = wp.table_frame.loc[wp.table_frame['datasetId']=='full','locator'][3]

query_requests = [{'query':base_query%(wp.loc,\
                    codestr,\
                    dat.loc[i,'LEADER'],\
                    dat.loc[i,'actor1country']),\
                    "leader":dat.loc[i,'LEADER'],"country":dat.loc[i,'actor1country']}\
                    for i in range(len(dat))]
query_requests
for i,dx in enumerate(query_requests):
    dx["req"]=jerbs.query(projectId=wp.proj_id,body={
    "query":dx['query']
      })





print query_requests[0]['query']




class REQDF(object):
    def __init__(self,req):
        self.country = req['country']
        self.leader = req['leader']
        self.dat = ""
        self.schema = ""
        self.query = req['query']
        self.query_req = req['req']
        self.req_to_df()
        print self.leader
        print self.country
    def req_to_df(self):
        _query = self.query_req.execute()
        wait_time = 0
        while not _query['jobComplete'] == True and wait_time<10:
            time.sleep(3)
            _query = self.query_req.execute()
            wait_time+=1
        _rows = [i['name'] for i in _query['schema']['fields']]
        if not _query['totalRows'] == u'0':
            _dat = pd.DataFrame([[j['v'] for j in i['f']] for i in _query['rows'] ])
            _dat.columns = _rows
            self.dat = _dat
        else:
            print _rows
            print pd.DataFrame([[0 for i in _rows]])
            _dat = pd.DataFrame([[0 for i in _rows]])
            _dat.columns = _rows
            _dat['Actor1name'] = self.leader
            _dat['Country'] = self.country
            self.dat = _dat
        self.schema = _rows
        return self.dat


x = REQDF(query_requests[0])
x = []
print x.query
for i in query_requests:
    x.append(REQDF(i).dat)


x
