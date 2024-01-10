import time
from sqlalchemy import create_engine
import boto3
import pandas as pd
import pandas as pd
import numpy as np
import airflow
import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
from datetime import date
from elasticsearch import Elasticsearch,RequestsHttpConnection,helpers
from elasticsearch.helpers import bulk
import time
import os
import io
import paramiko
from urllib.parse import urlparse
import psycopg2

#Airflow Parameters
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
email_aws_access_key_id = Variable.get("email_aws_access_key_id")
email_aws_secret_access_key = Variable.get("email_aws_secret_access_key")
email_aws_region = Variable.get("email_aws_region")
target_ingestion_endpoint = Variable.get("target_ingestion_endpoint")
target_ingestion_username = Variable.get("target_ingestion_username")
target_ingestion_password = Variable.get("target_ingestion_password")
sender = Variable.get("sender")
ba_recipient = Variable.get("bactpa_ba_recipients",deserialize_json=True)
dev_recipient = Variable.get("dev_recipient",deserialize_json=True)
client_recipient = Variable.get("client_recipient",deserialize_json=True)
bucket_name = Variable.get("bactpa_bucket_name")

#lob specific configuration
host = 'search-pricelist-bvgvrzu4rbe4iuguxgj2ei5z5e.us-east-1.es.amazonaws.com' #without 'https'
name = 'bactpa-out-of-network-ingestion'
headers_key = "raw/utils/allowed_amount_cols.csv"
billing_code_key = "raw/utils/billing_code.csv"
input_folder = "raw/vba_med/"
bactpa_hostname="sftp.bactpa.com"
bactpa_username="MPower_BAC"
bactpa_password="cyu$FtM3XeU9WSn"
bactpa_port=22
vba_hostname="sftp.nsp.cld.vbasoftware.net"
vba_username="Mpowered.BAC"
vba_password="T9NmmuFDnqHB8ZEg"
vba_port=56022
vba_sftp_folder = 'Mpowered/MED_OON_Outbound/'
download_path = '/home/airflow/'
process_start_time = time.time()
mpowered_vba_schema = "data_validation"
mpowered_vba_table = "allowed_amount_raw"

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

default_args = {
    "owner": "Airflow"
    }

start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id="bactpa-vba-med-ingestion",
    catchup = False,
    start_date = start_date,
    default_args=default_args,
    schedule_interval=None,
)

def send_mail(file_name,condition):
    if condition =="success":
        CONFIGURATION_SET = "ConfigSet"
        CHARSET = "UTF-8"
        subject = "Out of Network File Ingestion Success "+file_name
        body = """<!DOCTYPE html><html> <body> <p>Hi BAC team,</p> <p>"""+file_name+""" is successfully ingestion on """ +str(datetime.now())+ """.</p> <p>In case of any queries please contact MPH support team @ support@mpoweredhealth.com.</p>"""+   """<p>Regards,</p><p>MPH Team</p> </body></html>"""
        mail_client = boto3.client('ses',aws_access_key_id=email_aws_access_key_id, 
                aws_secret_access_key=email_aws_secret_access_key,region_name=email_aws_region)
        mail_client.send_email(Destination={'ToAddresses':ba_recipient['emails']+dev_recipient['emails']+client_recipient['emails']},
        Message={'Body':{'Html':{'Charset':CHARSET,'Data':body},},'Subject':{'Charset':CHARSET,'Data':subject},},Source=sender,)
    else:
        CONFIGURATION_SET = "ConfigSet"
        CHARSET = "UTF-8"
        subject = "Out of Network File Ingestion Failure "+file_name
        body = """<!DOCTYPE html><html> <body> <p>Hi BAC team,</p> <p>"""+file_name+""" was already successfully ingested before.</p> <p>In case of any queries please contact MPH support team @ support@mpoweredhealth.com.</p>"""+   """<p>Regards,</p><p>MPH Team</p> </body></html>"""
        mail_client = boto3.client('ses',aws_access_key_id=email_aws_access_key_id, 
                aws_secret_access_key=email_aws_secret_access_key,region_name=email_aws_region)
        mail_client.send_email(Destination={'ToAddresses':ba_recipient['emails']+dev_recipient['emails']+client_recipient['emails']},
        Message={'Body':{'Html':{'Charset':CHARSET,'Data':body},},'Subject':{'Charset':CHARSET,'Data':subject},},Source=sender,)

def data_extract(ds, **kwargs):
    def set_sftp_conn(host, port, username, password):
        transport = paramiko.Transport((host, port))
        print("connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection established.")
        return sftp
    bactpa_sftp = set_sftp_conn(bactpa_hostname,bactpa_port,bactpa_username,bactpa_password)
    bactpa_files = bactpa_sftp.listdir(vba_sftp_folder)
    print(bactpa_files)
    bactpa_files.sort(reverse=True)
    file_name = bactpa_files[0]
    print("Downloading the file")
    bactpa_sftp.get(vba_sftp_folder+file_name,download_path+file_name)
    print("Downloaded the file")
    s3_resource.Bucket(bucket_name).upload_file(download_path+file_name,input_folder+file_name)
    print("Uploading the file to s3")

def data_transform(ds, **kwargs):
    def set_sftp_conn(host, port, username, password):
        transport = paramiko.Transport((host, port))
        print("connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection established.")
        return sftp
    bactpa_sftp = set_sftp_conn(bactpa_hostname,bactpa_port,bactpa_username,bactpa_password)
    bactpa_files = bactpa_sftp.listdir(vba_sftp_folder)
    print(bactpa_files)
    bactpa_files.sort(reverse=True)
    file_name = bactpa_files[0]
    headers_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=headers_key)
    os.system("aws s3 mv s3://"+bucket_name+"/"+input_folder+file_name+" s3://"+bucket_name+"/archive/"+file_name)
    col_names = pd.read_csv(headers_file['Body'])['Column'].tolist()
    billing_code_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=billing_code_key)
    billing_code_list = pd.read_csv(billing_code_file['Body'])['Code'].tolist()
    # input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_vba_med_key)       
    final_list = ['service_name','coding_system','billing_code','coding_system_version','description','tin',
    'service_code','place_of_service_name','billing_class','allowed_amount','billed_charge','npi','modifier']
    current_list = ['prv_in_network_flag','prv_npi','svc_cpt_code','svc_hcpcs_code',
                       'svc_service_frm_date','svc_cpt_desc','svc_hcpcs_desc','svc_hcpcs_long_desc',
                       'svc_cpt_long_desc','prv_tin','svc_pos_code','svc_pos_desc','rev_claim_type_flag',
                       'rev_allowed_amt','svc_modifier_code','svc_modifier_2_code',
                       'svc_modifier_3_code','rev_billed_amt']
    first=True
    for df in pd.read_csv(download_path+file_name,encoding='ISO-8859-1',names=col_names,header=None,sep="|",chunksize=100000):
        data = df[current_list]
        data = data[data['prv_in_network_flag']==1]
        data = data[data['prv_npi'].notnull()]
        data1 = data[data.svc_cpt_code.isin(billing_code_list)]
        data2 = data[data.svc_hcpcs_code.isin(billing_code_list)]
        data = pd.concat([data1,data2],axis=0)
        print(data['svc_service_frm_date'].iloc[0])
        print(data.dtypes['svc_service_frm_date'])
        data['Date'] = pd.to_datetime(data['svc_service_frm_date'],format='%m/%d/%Y')
        current_date = file_name.split('.')[0].split('_')[3]
        data['Current_Date'] = datetime.strptime(current_date,"%Y%m%d")
        data['Delta_Date'] = data['Current_Date'] - data['Date']
        data = data[data['Delta_Date'].dt.days>=90]
        data = data[data['Delta_Date'].dt.days<=180]
        data['svc_cpt_code'].fillna('',inplace=True)
        data['service_name'] = np.where(data['svc_cpt_code']=='',data['svc_hcpcs_desc'],data['svc_cpt_desc'])
        data['coding_system'] = np.where(data['svc_cpt_code']=='',"HCPCS","CPT")
        data['billing_code'] = np.where(data['svc_cpt_code']=='',data['svc_hcpcs_code'],data['svc_cpt_code'])
        data['coding_system_version'] = pd.DatetimeIndex(data['svc_service_frm_date']).year
        data['description'] = np.where(data['svc_cpt_code']==None,data['svc_hcpcs_long_desc'],data['svc_cpt_long_desc'])
        data = data[data['billing_code'].notnull()]
        data['description'].fillna('Not Available',inplace=True)
        data['tin'] = data['prv_tin']
        data['service_code'] = data['svc_pos_code']
        data['place_of_service_name'] = data['svc_pos_desc']
        data['billing_class'] = np.where(data["rev_claim_type_flag"]=='0','Professional',np.where(data["rev_claim_type_flag"]=='1','Institutional','Dental'))
        data['allowed_amount'] = data['rev_allowed_amt']
        data['modifier'] = [[e for e in row if e==e] for row in data[['svc_modifier_code','svc_modifier_2_code','svc_modifier_3_code']].values.tolist()]
        data['billed_charge'] = data['rev_billed_amt']
        data['npi'] = data['prv_npi'].map(lambda x : int(x))
        data['allowed_amount'] = data['allowed_amount'].map(lambda x: float(x))
        data = data[data['allowed_amount']>=0]
        if first:
            final_data = data[final_list]
            first = False
        else:
            final_data = pd.concat([final_data,data[final_list]])
        print("Current Length = {}".format(len(final_data)))    
    
    csv_buffer = io.StringIO()
    final_data['modifier'] = final_data['modifier'].map(lambda x : str(x))
    final_data = final_data.drop_duplicates()
    final_data.to_csv(csv_buffer,index=False)
    print(len(final_data))
    os.system('rm '+download_path+file_name)
    s3_resource.Object(bucket_name,'output/'+file_name).put(Body=csv_buffer.getvalue())
    print("Time taken for data processing = {} mins".format(str(int(time.time()-process_start_time)/60)))


def ingest_to_elastic_search(ds, **kwargs):
    start_time = time.time()

    def generator(index_name,final_data):
        final_data_iter = final_data.iterrows()
        for index,document in final_data_iter:
            yield {
                '_index':index_name,
                '_type':'_doc',
                '_source': document.to_dict()
            }
    def get_output_and_store(final_data):
        start_time = time.time()
        for success,info in helpers.parallel_bulk(client=es,actions=generator(index_name,final_data),chunk_size=100,thread_count=8,queue_size=8):
            if not success:
                print('Doc failed',info)
        print("uploaded in {} minutes".format(str(int(time.time()-start_time)/60)))
    

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=("mpowered", "Mpowered@123"),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    # print(es.info())

    Settings = {
        "settings": {
        "number_of_shards": 1,
        "index.mapping.nested_objects.limit":1000000
        },
        "mappings": {
            "dynamic": "true",
            "properties":{
                "billing_code": {
                    "type": "keyword"
                },
                "billing_class": {
                    "type": "keyword"
                },
                "service_code": {
                    "type": "keyword"
                },
                "modifier":{
                    "type":"keyword"
                    },
                "npi": {
                    "type": "keyword"
                },
                "service_name": {
                    "type": "text",
                    "fields":{
                        "keyword":{
                            "type":"keyword"
                        }
                    }
                },
                "allowed_amount": {
                    "type": "float"
                    },
            }
        }
    }
    def set_sftp_conn(host, port, username, password):
        transport = paramiko.Transport((host, port))
        print("connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection established.")
        return sftp
    bactpa_sftp = set_sftp_conn(bactpa_hostname,bactpa_port,bactpa_username,bactpa_password)
    bactpa_files = bactpa_sftp.listdir(vba_sftp_folder)
    print(bactpa_files)
    bactpa_files.sort(reverse=True)
    file_name = bactpa_files[0]
    index_name = "price_list_plan_out_2_" + file_name.split('.')[0].split('_')[3]
    try:
        my =es.indices.create(index=index_name,body=Settings)
    except:
        send_mail(file_name,"failure")
        raise AirflowException("File Already Ingested Before")
    res = es.indices.get_alias("*")
    output_key = 'output/'+file_name
    output_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=output_key)
    data = pd.read_csv(output_file['Body'])
    get_output_and_store(data)
    print(index_name)
    es.indices.update_aliases({
        "actions":[
            {"add":{"index":index_name,"alias":"price_list_plan_out_2"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_3"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_4"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_8"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_8"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_mmo"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_healthlink"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_cigna"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_sagamore"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_aetna"}},
            {"add":{"index":index_name,"alias":"price_list_plan_out_primehealth"}},
        ]
    })
    send_mail(file_name,"success")

task_data_extract = PythonOperator(
    task_id='task_data_extract',
    python_callable=data_extract,
    provide_context=True,
    dag=dag)

task_data_transform = PythonOperator(
    task_id='task_data_transform',
    python_callable=data_transform,
    provide_context=True,
    dag=dag)

task_ingest_to_elastic_search = PythonOperator(
    task_id='task_ingest_to_elastic_search',
    python_callable=ingest_to_elastic_search,
    provide_context=True,
    dag=dag)

task_data_extract >> task_data_transform >> task_ingest_to_elastic_search