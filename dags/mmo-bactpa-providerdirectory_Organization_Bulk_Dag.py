
from fhir.resources.organization import Organization
from fhir.resources.identifier import Identifier
import requests
import pandas as pd
import json 
import concurrent.futures
import time
from zipfile import ZipFile
import boto3
from io import StringIO 
import airflow
import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import Variable

#lob specific configuration
organization_name = 'mmo'
aws_access_key_id = "AKIARL4635R4OPW5FOXP"
aws_secret_access_key = "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"
affliate_name = 'bactpa'
lob_name ='providerdirectory'
tenant_name = 'ProviderDirectory'
resources = ["Practitioner","Organization","Location","PractitionerRole"]
actualresource = 'Organization'
file_type = 'tab'
name = 'mmo-bactpa-providerdirectory'
bucket_name = name+'-data-ingestion'
schedule_interval = None
input_folder = "raw/"
download_path =  "/home/airflow/"

columns_in_use = []

headers = ["PROV_NAME","PROV_LAST_NAME","PROV_FIRST_NAME","PROV_MID_INIT","PROV_TITLE","SERV_NAME",
"SERV_ADDR_LINE_1","SERV_CITY","SERV_STATE","SERV_ZIP_CD","APPT_PHONE_AREA_CD formatted (###)",
"APPT_PHONE_NO formated  999-9999","DESCO","BOARD_CERT_FL","NEW_PATNT_IN","PROV_NPI"]

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

default_args = {
    "owner": "Airflow"
    }

columns_in_use = []
start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id=name+'_'+actualresource+'_Bulk_Dag',
    schedule_interval = None,
    catchup = False,
    start_date = start_date,
    default_args=default_args
)



def data_extract(ds,**kwargs):
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('zip'):
            s3_client.download_file(bucket_name,objects.key,download_path+objects.key.split('/')[1])
            with ZipFile(download_path+objects.key.split('/')[1],'r') as zObject:
                zObject.extractall(path=download_path) 

def concatenate(ds,**kwargs):
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('zip'):
            file_name = objects.key.split('/')[1]
    files = [download_path+file_name.split('.')[0]+'.txt']
    file_dict = []
    for key in files:
        if ".csv" in key:
            file_name = key.split('/')[-1][:-4]
            csv_file = pd.read_csv(key)
            if len(csv_file)>0:
                csv_file['file_name'] = file_name
                file_dict.append(csv_file)
        elif ".txt" in key:
            file_name = key.split('/')[-1][:-4]
            csv_file = pd.read_csv(key,sep='\t',skipinitialspace=True,names=headers)
            if len(csv_file)>0:
                csv_file['file_name'] = file_name
                file_dict.append(csv_file)
    for col in csv_file.columns:
        csv_file[col] = csv_file[col].apply(lambda x: str(x).strip())
    if len(file_dict) == 0:
        message = "input csv file not present or empty in the raw location"
    input_df = pd.concat(file_dict)
    input_df['batch_index'] = input_df.index
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'concatenated/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

def standardization_mapped(ds,**kwargs):
    columns_in_use = []
    input_key = "concatenated/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    input_df['identifier_1_system'] = "https://github.com/synthetichealth/synthea"
    input_df['type_coding_0_system'] = "http://terminology.hl7.org/CodeSystem/organization-type"
    input_df['type_coding_0_display'] = 'Healthcare Provider'
    input_df['extension_0_url'] = "http://synthetichealth.github.io/synthea/utilization-encounters-extension"
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'mapped/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())


def ingest_to_smilecdr(ds,**kwargs):
    input_key = "mapped/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    ingested_organizations = []
    def ingest(i):
        row = input_df.iloc[i]
        organization_name = row['name']
        organization_name = organization_name.replace('&','and')
        organization_name = organization_name.replace('#','%23')
        organization_name = organization_name.replace('+','plus')
        organization_name = organization_name.replace(',',' ')
        try:
            organization_response1 = requests.get(target_ingestion_endpoint+tenant_name+'/'+actualresource+'?name:exact='+organization_name,
                                auth=(target_ingestion_username , target_ingestion_password))
            organization_result1 = organization_response1.json()
            total = organization_result1['total']
            # id = organization_result1['entry'][0]['resource']['id']
        except:
            print("Not even cigna organization found = {}".format(actualresource+'?identifier=mmo&name:exact='+organization_name))
        organization_result1 = organization_response1.json()
        if organization_result1['total']>0:
            try:
                organization_response2 = requests.get(target_ingestion_endpoint+tenant_name+'/'+actualresource+'?identifier=mmo&name:exact='+organization_name,
                                auth=(target_ingestion_username , target_ingestion_password)) 
                organization_result2 = organization_response2.json()
                total = organization_result2['total']
            except:
                print("cigna organization found but not mmo= {}".format(actualresource+'?identifier=mmo&name:exact='+organization_name))
            organization_result2 = organization_response2.json()
            if organization_result2['total']==0:
                resource = organization_result1['entry'][0]['resource']
                identifier = resource['identifier']
                identifier.append({
                        "use": "secondary",
                        "system": "eam-prov-dir-partner-name",
                        "value": "mmo"
                    })
                resource['identifier'] = identifier
                try:
                    response = requests.put(target_ingestion_endpoint+tenant_name+'/'+actualresource+'/'+resource['id'],
                            json=resource,
                            auth=(target_ingestion_username , target_ingestion_password)).json()
                    id = response['id']
                except:
                    print(resource['id'])
            # elif organization_result2['total']>1:
            #     for i in range(1,organization_result2['total']):
            #         id = organization_result2['entry'][i]['resource']['id']
            #         try:
            #             response = requests.delete(target_ingestion_endpoint+tenant_name+'/'+actualresource+'/'+id,
            #                                     auth=(target_ingestion_username , target_ingestion_password))
            #         except:
            #             print(id)
        else:
            organization = Organization.construct()
            identifier1 = Identifier.construct()
            identifier2 = Identifier.construct()
            organization.identifier = list()
            identifier1.system = row['identifier_system']
            identifier1.value = row['identifier_value']
            identifier2.system = row['identifier_system1']
            identifier2.value = row['identifier_value1']
            identifier2.use = row['identifier_use1']
            organization.identifier.append(identifier1)
            organization.identifier.append(identifier2)
            name = row['name'].replace('&','and')
            name = name.replace('+','plus')
            name = name.replace(',',' ')
            organization.name = name
            organization.active = row['active']
            try:
                response = requests.post(target_ingestion_endpoint+tenant_name+'/'+actualresource,
                                json=organization.dict(),
                                auth=(target_ingestion_username , target_ingestion_password)).json()
                id = response['id']
            except:
                ingested_organizations.append(row['name'])
                print(row['name'])               
        if i%1000==0:
            print("rows processed = {}".format(i))
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        res = [executor.submit(ingest,i) for i in range(len(input_df))]
        concurrent.futures.wait(res)
        print("No of new Organizations ingested = {}".format(len(ingested_organizations)))
        print(ingested_organizations)
    # for i in range(10):
    #     ingest(i)

task_data_extract = PythonOperator(
    task_id='task_data_extract',
    python_callable=data_extract,
    provide_context=True,
    dag=dag)

task_concatenate = PythonOperator(
    task_id='task_concatenate',
    python_callable=concatenate,
    provide_context=True,
    dag=dag)

task_standardization_mapped = PythonOperator(
    task_id='task_standardization_mapped',
    python_callable=standardization_mapped,
    provide_context=True,
    dag=dag)

task_ingest_to_smilecdr = PythonOperator(
    task_id='task_ingest_to_smilecdr',
    python_callable=ingest_to_smilecdr,
    provide_context=True,
    dag=dag)


task_data_extract >> task_concatenate >> task_standardization_mapped >> task_ingest_to_smilecdr
