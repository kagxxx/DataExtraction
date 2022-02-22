import json
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin, SFType
from django.http import HttpResponse
from django.shortcuts import render
import datetime
import os


def index(request):
    return render(request, 'index.html')


# Getting data from salesforce
loginInfo = json.load(open('login.json'))
username = loginInfo['username']
password = loginInfo['password']
security_token = loginInfo['security_token']
domain = 'login'

# sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)

session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token,
                                       domain=domain)
sf = Salesforce(instance=instance, session_id=session_id)

for element in dir(sf):
    if not element.startswith('_'):
        if isinstance(getattr(sf, element), str):
            print('Property Name:{0} ;Value: {1}'.format(element, getattr(sf, element)))

metadata_org = sf.describe()
print(metadata_org['encoding'])
print(metadata_org['maxBatchSize'])
print(metadata_org['sobjects'])
df_sobjects = pd.DataFrame(metadata_org['sobjects'])
df_sobjects.to_csv('org metadata info.csv', index=False)

project__c = sf.Project__c
metadata_project = project__c.metadata()
df_project_metadata = pd.DataFrame(metadata_project.get('objectDescribe'))
df_project_metadata.to_csv('project metadata.csv', index=False)

# Adding to the database
querySOQL = """SELECT Id, Name, StageName, Account.Name, Account.Type, Account.Industry FROM Opportunity"""

response = sf.query(querySOQL)
lstRecords = response.get('records')
nextRecordsUrl = response.get('nextRecordsUrl')

while not response.get('done'):
    response = sf.query_more(nextRecordsUrl, identifier_is_url=True)
    lstRecords.extend(response.get('records'))
    nextRecordsUrl = response.get('nextRecordsUrl')

df_records = pd.DataFrame(lstRecords)
dfAccount = df_records['Account'].apply(pd.Series).drop(labels='attributes', axis=1, inplace=False)
dfAccount.columns = ('Account.{0}'.format(name) for name in dfAccount.columns)

df_records.drop(labels=['Account', 'attributes'], axis=1, inplace=True)

dfOpptyAcct = pd.concat([df_records, dfAccount], axis=1)
dfOpptyAcct.to_csv('Oppty to Acct.csv', index=False)

"""
SOSL Query Call
"""
records = sf.search('FIND {United Oil Installations} RETURNING Opportunity (Id, Name, StageName)')

# Updating the database
project__c = SFType('Project__c', session_id, instance)

today = datetime.datetime.now()

data = {
    'Name': 'Project Yellowstone',
    'Priority__c': 'High',
    'Start_Date__c': today.isoformat() + 'Z',
    'End_Date__c': (today + datetime.timedelta(days=45)).isoformat() + 'Z'
}

response = project__c.create(data)
print(response)

"""
Parent-Child Relationship Record creation
"""
project__c = SFType('Project__c', session_id, instance)
account = SFType('Account', session_id, instance)

for i in range(1, 6):
    data_account = {'Name': 'Retail Account ' + str(i), 'Type': 'Prospect'}
    response_account = account.create(data_account)
    accountId = response_account.get('id')

    data_project = {'Name': 'Project Yosemite ' + str(i), 'Customer_Account__c': accountId}
    response_project = project__c.create(data_project)
    projectId = response_project.get('id')

    print('Record Created')
    print('-'.center(50, '-'))
    print('Account Id: {0}'.format(accountId))
    print('Project Id: {0}'.format(projectId))

"""
Update Record
"""


def convert_to_RFC_datetime(year=1900, month=1, day=1, hour=0, minute=0):
    dt = datetime.datetime(year, month, day, hour, minute, 0).isoformat() + 'Z'
    return dt


year = 2020
month = 5
day = 15

update_data = {}
update_data['Budget__c'] = 300_000
update_data['Start_Date__c'] = convert_to_RFC_datetime(year, month, day)
update_data['End_Date__c'] = convert_to_RFC_datetime(year, month + 5, 1)
update_data['Priority__c'] = 'Medium'

response_project.get('id')

project__c.update(response_project.get('id'), update_data)

# Downloading Files

querySOQL = """
          SELECT Id, Name, ParentId, Body 
          From Attachment 
          WHERE ParentId IN (SELECT Id FROM Project__c)
          """

# query records method
response = sf.query(querySOQL)
lstRecords = response.get('records')
nextRecordsUrl = response.get('nextRecordsUrl')

while not response.get('done'):
    response = sf.query_more(nextRecordsUrl, identifier_is_url=True)
    lstRecords.extend(response.get('records'))
    nextRecordsUrl = response.get('nextRecordsUrl')

df_records = pd.DataFrame(lstRecords)

"""
Download files
"""

instance_name = sf.sf_instance
folder_path = '.\Attachments Download'

for row in df_records.iterrows():
    record_id = row[1]['ParentId']
    file_name = row[1]['Name']
    attachment_url = row[1]['Body']

    if not os.path.exists(os.path.join(folder_path, record_id)):
        os.mkdir(os.path.join(folder_path, record_id))

    request = sf.session.get('https://{0}{1}'.format(instance_name, attachment_url), headers=sf.headers)
    with open(os.path.join(folder_path, record_id, file_name), 'wb') as f:
        f.write(request.content)
        f.close()
