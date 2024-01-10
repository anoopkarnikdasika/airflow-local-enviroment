import time
import airflow
import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
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
from datetime import datetime


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
vba_sftp_folder = 'Mpowered/EDI_834_Outbound/'
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
    schedule_interval=None,
    dag_id="bactpa-834-ingestion",
    catchup = False,
    start_date = start_date,
    default_args=default_args
)

network_mapping = {"CGHAP":"cigna","Healthlink":"healthlink","Sag":"sagamore",
"CGHCAP":"cigna","MMO":"mmo","CIGNA":"cigna","FH":"firsthealth","CGPHMI":"cigna",
"CGMHP":"cigna"}


def send_mail(file_name):
    CONFIGURATION_SET = "ConfigSet"
    CHARSET = "UTF-8"
    subject = "834 File Ingestion Success "+file_name
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
    sftp = set_sftp_conn(bactpa_hostname,bactpa_port,bactpa_username,bactpa_password)
    files = sftp.listdir(vba_sftp_folder)
    print(files)
    files.sort(reverse=True)
    file_name = files[0]
    sftp.get(vba_sftp_folder+file_name,download_path+file_name)
    s3_resource.Bucket(bucket_name).upload_file(download_path+file_name,input_folder+file_name)
    os.system('rm '+download_path+file_name)

def data_transform_and_ingest(ds, **kwargs):
    def set_sftp_conn(host, port, username, password):
        transport = paramiko.Transport((host, port))
        print("connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection established.")
        return sftp
    sftp = set_sftp_conn(bactpa_hostname,bactpa_port,bactpa_username,bactpa_password)
    files = sftp.listdir(vba_sftp_folder)
    print(files)
    files.sort(reverse=True)
    file_name = files[0]
    obj = s3_resource.Object(bucket_name,input_folder+file_name)
    transactions = []
    conn = psycopg2.connect(
         "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
        "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute("select id from "+mpowered_organization_table+" where name='"+org_name+"'")
    result = cursor.fetchall();
    org_id = result[0][0]
    print(org_id)
    cursor.close()
    conn = psycopg2.connect(
        "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
        "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute("delete from "+mpowered_bac_member_table)
    conn.commit()
    cursor.close()
    print(network_mapping.keys())
    for line in obj.get()['Body'].iter_lines():
        words = line.decode('utf8').strip().split('*')
        if words[0] == 'ST':
            transaction = {}
            transaction['subscriber_id'] = ''
            transaction['org_id'] = org_id
            additional_info = {}
            additional_info['network'] = ''
        elif words[0] == 'NM1' and words[1] == 'IL':
            transaction['subscriber_id'] = words[-1][:-1]
        elif words[0] == 'REF' and words[1] == 'ZZ':
            if words[-1][:-1] in network_mapping.keys():
                additional_info["network"] = network_mapping[words[-1][:-1]]
            else:
                additional_info["network"] = "Not Available"
            transaction['additional_info'] = json.dumps(additional_info)
            transaction['deductible_value'] = 0
        elif words[0] == 'SE':
            transactions.append(transaction)
    print(len(transactions))
    # data = pd.DataFrame(transactions)
    # data.to_csv("load.csv",index=False)
    # csv_buffer = io.StringIO()
    # data.to_csv(csv_buffer,index=False)
    # s3_resource.Object(bucket_name,'output/834_'+current_date+'.csv').put(Body=csv_buffer.getvalue())
    conn = psycopg2.connect(
        "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
        "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    query2 = "Insert into "+mpowered_bac_member_table+" (subscriber_id,org_id,additional_info,deductible_value) Values %s"
    values = [[value for value in transaction.values()] for transaction in transactions]
    execute_values(cursor,query2,values)
    conn.commit()
    cursor.close()
    conn = psycopg2.connect(
        "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
        "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute("""delete from """+mpowered_bac_member_table+""" where 
    additional_info ->> 'network' = 'Not Available';""")
    conn.commit()
    cursor.close()
    copy_source= {
        'Bucket': bucket_name,
        'Key': input_folder+file_name
    }
    s3_resource.meta.client.copy(copy_source,bucket_name,'processed/834/'+file_name)
    send_mail(file_name)

task_data_extract= PythonOperator(
    task_id='task_data_extract',
    python_callable=data_extract,
    provide_context=True,
    dag=dag
)

task_data_transform_and_ingest = PythonOperator(
    task_id='task_data_transform_and_ingest',
    python_callable=data_transform_and_ingest,
    provide_context=True,
    dag=dag
)

task_data_extract >> task_data_transform_and_ingest