
from fhir.resources.practitioner import Practitioner
from fhir.resources.identifier import Identifier
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.humanname import HumanName
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

def isNan(num):
    return num!=num

#lob specific configuration
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
organization_name = 'mmo'
affliate_name = 'bactpa'
lob_name ='providerdirectory'
tenant_name = 'ProviderDirectory'
resources = ["Practitioner","Organization","Location","PractitionerRole"]
actualresource = 'Practitioner'
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
    mapping_key = "mapping/" + actualresource+"/"+ actualresource + "_Mapping.csv"
    mapping_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=mapping_key)
    mapping_df = pd.read_csv(mapping_file['Body'])
    mapping_df_filtered = mapping_df.iloc[:, 0:2].transpose()
    new_header = mapping_df_filtered.iloc[1]
    mapping_df_filtered = mapping_df_filtered[0:1]
    mapping_df_filtered.columns = new_header
    mapping_df_filtered.rename(columns=mapping_df_filtered.iloc[0])
    mapping_dict = mapping_df_filtered.to_dict(orient='list')
    mapping_dict_unique = {}
    mapping_dict_multiple = {}
    mod_keys = {}
    for key,value in mapping_dict.items():
        cur_value = str(value[0])
        if cur_value!='nan' and '+' not in cur_value:
            columns_in_use.append(key)
            if '/' in cur_value:
                cur_values = cur_value.split('/')
                for i in range(len(cur_values)):
                    mapping_dict_unique[key+str(i)] = [cur_values[i].strip()]
                mod_keys[key] = len(cur_values)
            else:
                if cur_value not in mapping_dict_multiple:
                    mapping_dict_unique[key] = value
                    mapping_dict_multiple[cur_value] = [key]
                else:
                    mapping_dict_multiple[cur_value].append(key)
        elif '+' in cur_value:
            cur_values = cur_value.split('+')
            columns_in_use.append(key)
            if len(cur_values) ==2:
                input_df[key] = input_df[cur_values[0]].astype('string') + ' ' + input_df[cur_values[1]].astype('string')
            elif  len(cur_values) ==3:
                input_df[key] = input_df[cur_values[0]].astype('string') + ' ' + input_df[cur_values[1]].astype('string') + ' ' + input_df[cur_values[2]].astype('string')
            elif  len(cur_values) ==4:
                input_df[key] = input_df[cur_values[0]].astype('string') + ' ' + input_df[cur_values[1]].astype('string') + ' ' + input_df[cur_values[2]].astype('string') + ' ' + input_df[cur_values[3]].astype('string') 
    for key,value in mapping_dict_unique.items():
        input_df[key] = input_df[value[0]]
    for key,values in mapping_dict_multiple.items():
        if len(values)>1:
            for i in range(len(values)-1):
                input_df[values[i+1]] = input_df[values[0]]    
    def conv(data,mod_key,length):
        for a in range(length):
            if str(data[mod_key+str(a)])!='nan':
                return data[mod_key+str(a)]
    for mod_key in mod_keys.keys():
        input_df[mod_key] = input_df.apply(lambda x: conv(x,mod_key,mod_keys[mod_key]),axis=1)
        for i in range(mod_keys[mod_key]):
            input_df.drop(mod_key+str(i),inplace=True,axis=1)
    for i in range(len(mapping_df)):
        if not pd.isna(mapping_df['fixed_values'].iloc[i]):
            columns_in_use.append(mapping_df['tgt'].iloc[i])
            input_df[mapping_df['tgt'].iloc[i]] = mapping_df['fixed_values'].iloc[i]
    input_df = input_df[columns_in_use]
    print(len(input_df))
    input_df = input_df.drop_duplicates(subset=['identifier_value'])
    print(len(input_df))
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'mapped/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

def ingest_to_smilecdr(ds,**kwargs):
    input_key = "mapped/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    ingested_practitioners = []
    def ingest(j):
        row = input_df.iloc[j]
        npi = str(int(row['identifier_value']))
        try:
            practitioner_response1 = requests.get(target_ingestion_endpoint+tenant_name+'/'+actualresource+'?identifier='+npi,
                                auth=(target_ingestion_username , target_ingestion_password))
        except:
            print(actualresource+'?identifier='+npi)
        practitioner_result1 = practitioner_response1.json()
        if practitioner_result1['total']>0:
            try:
                practitioner_response2 = requests.get(target_ingestion_endpoint+tenant_name+'/'+actualresource+'?identifier=mmo&identifier='+npi,
                                auth=(target_ingestion_username , target_ingestion_password))
            except:
                print(actualresource+'?identifier=mmo&identifier='+npi)
            practitioner_result2 = practitioner_response2.json()
            if practitioner_result2['total']==0:
                resource = practitioner_result1['entry'][0]['resource']
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
            elif practitioner_result2['total']>1:
                print(target_ingestion_endpoint+tenant_name+'/'+actualresource+'?identifier=mmo&identifier='+npi)
                # for i in range(1,practitioner_result2['total']):
                #     id = practitioner_result2['entry'][i]['resource']['id']
                #     try:
                #         response = requests.delete(target_ingestion_endpoint+tenant_name+'/'+actualresource+'/'+id,
                #                                 auth=(target_ingestion_username , target_ingestion_password))
                #     except:
                #         print(id)
            else:
                resource = practitioner_result2['entry'][0]['resource']
                if 'given' not in resource['name'][0]:
                    resource['name'][0]['given'] = []
                    if not isNan(row['name_given']):
                        resource['name'][0]['given'].append(row['name_given'])
                    try:
                        response = requests.put(target_ingestion_endpoint+tenant_name+'/'+actualresource+'/'+resource['id'],
                                json=resource,
                                auth=(target_ingestion_username , target_ingestion_password)).json()
                        id = response['id']
                    except:
                        print(resource['id'])


        else:
            practitioner = Practitioner.construct()
            codeableconcept = CodeableConcept.construct()
            codeableconcept.coding = list()
            coding = Coding.construct()
            coding.system = row['identifier_type_coding_system']
            coding.version = row['identifier_type_coding_version']
            coding.code = row['identifier_type_coding_code']
            coding.display = row['identifier_type_coding_display']
            codeableconcept.coding.append(coding)
            identifier1 = Identifier.construct()
            identifier2 = Identifier.construct()
            identifier3 = Identifier.construct()
            practitioner.identifier = list()
            identifier1.type = codeableconcept
            identifier1.value= int(row['identifier_value'])
            identifier2.system = row['identifier_system1']
            identifier2.value = row['identifier_value1']
            identifier3.system = row['identifier_system2']
            identifier3.value = row['identifier_value2']
            identifier3.use = row['identifier_use2']
            practitioner.identifier.append(identifier1)
            practitioner.identifier.append(identifier2)
            practitioner.identifier.append(identifier3)
            humanname = HumanName.construct()
            practitioner.name = list()
            if not isNan(row['name_text']):
                humanname.text = row['name_text']
            if not isNan(row['name_family']):
                humanname.family = row['name_family']
            humanname.given = list()
            if not isNan(row['name_given']):
                humanname.given.append(row['name_given'])
            humanname.prefix = list()
            if not isNan(row['name_prefix']):
                humanname.prefix.append(row['name_prefix'])
            practitioner.name.append(humanname)
            practitioner.active = True
            try:
                response = requests.post(target_ingestion_endpoint+tenant_name+'/'+actualresource,
                        json=practitioner.dict(),
                        auth=(target_ingestion_username , target_ingestion_password)).json()
                id = response['id']
                # print(response['identifier'][0]['value'])
            except:
                ingested_practitioners.append(row['name_text'])
                print(row['name_text'])
        if j%1000==0:
            print("rows processed = {}".format(j))
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        res = [executor.submit(ingest,i) for i in range(len(input_df))]
        concurrent.futures.wait(res)
        print("No of new Practitioners ingested = {}".format(len(ingested_practitioners)))
        print(ingested_practitioners)
    # for j in range(10):
    #     ingest(j)

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


task_concatenate >> task_standardization_mapped >> task_ingest_to_smilecdr
