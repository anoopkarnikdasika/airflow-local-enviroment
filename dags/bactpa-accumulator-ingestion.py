import time
import airflow
import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import boto3
import pandas as pd
import pandas as pd
import numpy as np
from datetime import datetime
from elasticsearch import Elasticsearch,helpers
from elasticsearch.helpers import bulk
import time
import os
import io
import json
import psycopg2
from psycopg2.extras import execute_values
import paramiko


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
client_recipient = Variable.get("bactpa_client_recipients",deserialize_json=True)
bucket_name = Variable.get("bactpa_bucket_name")
mpowered_host = Variable.get("mpowered_host")
mpowered_database = Variable.get("mpowered_database")
mpowered_password = Variable.get("mpowered_password")
mpowered_user = Variable.get("mpowered_user")
mpowered_port = Variable.get("mpowered_port")

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
vba_sftp_folder = 'Outbound/Accums/Test/'
download_path = '/home/airflow/'
process_start_time = time.time()
mpowered_vba_schema = "data_validation"
mpowered_vba_table = "allowed_amount_raw"
mpowered_bac_member_table = "member_details.members_834_file"
mpowered_organization_table = "partner_schema.organization"
org_name = 'bactpaorg'

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

default_args = {
    "owner": "Airflow"
    }

start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id="bactpa-accumulator-ingestion",
    catchup = False,
    start_date = start_date,
    default_args=default_args,
    schedule_interval=None,
)

def send_mail(file_name):
    
    CONFIGURATION_SET = "ConfigSet"
    CHARSET = "UTF-8"
    subject = "Accumulator File Ingestion Success "+file_name
    body = """<!DOCTYPE html><html> <body> <p>Hi BAC team,</p> <p>"""+file_name+""" is successfully ingestion on """ +str(datetime.now())+ """.</p> <p>In case of any queries please contact MPH support team @ support@mpoweredhealth.com.</p>"""+   """<p>Regards,</p><p>MPH Team</p> </body></html>"""
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
    sftp = set_sftp_conn(vba_hostname,vba_port,vba_username,vba_password)
    files = sftp.listdir(vba_sftp_folder)
    print(files)
    files.sort(reverse=True)
    file_name = files[0]
    print("Downloading the file")
    sftp.get(vba_sftp_folder+file_name,download_path+file_name)
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
    sftp = set_sftp_conn(vba_hostname,vba_port,vba_username,vba_password)
    files = sftp.listdir(vba_sftp_folder)
    print(files)
    files.sort(reverse=True)
    file_name = files[0]
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_folder+file_name)
    col_names = ["VersionNumber","GroupNumber","SSN","MemberID","AltMemberID","DepSSN","DepMemberID",
                   "DepAltMemberID","Name","Accum_Desc","CoverageType","SpecificType","CurrentAmount",
                   "MaximumAmount","InNetwork","IndividualAccum","CountMet","CountRequired","CountType",
                   "DateRange","ClaimSystemCode","BnftPlan","BenefitCode","PlanYear","ActionFlag"]
    data = pd.read_csv(input_file['Body'],names=col_names)
    data= data.sort_values('DateRange')
    data_latest = data.drop_duplicates(subset=['MemberID','DepMemberID'],keep='first')
    features = ['MemberID','DepMemberID','CurrentAmount','MaximumAmount']
    data_latest = data_latest[features]
    data_latest['MemberID'] = data_latest['MemberID'].apply(lambda x :str(x)) +"01"
    data_latest['DeductibleValue'] = data_latest['MaximumAmount'] - data_latest['CurrentAmount']
    data_latest['DeductibleValue'] = data_latest['DeductibleValue'].fillna(data_latest['MaximumAmount'])
    data_latest['DeductibleValue'] = data_latest['DeductibleValue'].fillna(0)
    data_latest['DepMemberID'] = data_latest['DepMemberID'].fillna(data_latest['MemberID'])
    data_latest['DepMemberID'] = data_latest['DepMemberID'].apply(lambda x :str(int(x)))
    data_latest['DeductibleValue'] = data_latest['DeductibleValue'].apply(lambda x:x if x>0 else 0)
    print(len(data_latest))
    csv_buffer = io.StringIO()
    data_latest.to_csv(csv_buffer,index=False)
    s3_resource.Object(bucket_name,'output/'+file_name).put(Body=csv_buffer.getvalue())


def ingest_to_postgres(ds, **kwargs):
    def set_sftp_conn(host, port, username, password):
        transport = paramiko.Transport((host, port))
        print("connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection established.")
        return sftp
    sftp = set_sftp_conn(vba_hostname,vba_port,vba_username,vba_password)
    files = sftp.listdir(vba_sftp_folder)
    print(files)
    files.sort(reverse=True)
    file_name = files[0]
    output_key = 'output/'+file_name
    output_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=output_key)
    data = pd.read_csv(output_file['Body'])
    for i in range(len(data)):
        deductible_value = data.iloc[i]['DeductibleValue']
        subscriber_id = data.iloc[i]['DepMemberID']
        conn = psycopg2.connect(
            "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
            "password='"+mpowered_password+"'")
        cursor = conn.cursor()
        cursor.execute(
            "update "+mpowered_bac_member_table+" set deductible_value="+str(deductible_value)+" where subscriber_id='"+str(int(subscriber_id))+"'")
        conn.commit()
        cursor.close()
        if i%100==0:
            print("Updated rows ={} with latest subscriber_id updated ={} with value={}".format(i,str(int(subscriber_id)),deductible_value))
        copy_source= {
            'Bucket': bucket_name,
            'Key': input_folder+file_name
        }
        s3_resource.meta.client.copy(copy_source,bucket_name,'processed/accumulator/'+file_name)
    send_mail(file_name)

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

task_ingest_to_postgres = PythonOperator(
    task_id='task_ingest_to_postgres',
    python_callable=ingest_to_postgres,
    provide_context=True,
    dag=dag)

task_data_extract >> task_data_transform >> task_ingest_to_postgres