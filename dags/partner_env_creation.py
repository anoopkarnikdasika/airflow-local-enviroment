from datetime import datetime
import datetime
import requests
from requests.auth import HTTPBasicAuth
import time
import boto3
from urllib.parse import urlparse
import pandas as pd
import re
import json
import psycopg2
import os 
import io
import airflow
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import Variable
#from fuzzywuzzy import process


#AWS Credentials
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
region_name = "ap-south-1"

#Mpowered Database
mpowered_host = Variable.get('mpowered_host')
mpowered_port = Variable.get('mpowered_port')
mpowered_database = Variable.get('mpowered_database')
mpowered_user = Variable.get('mpowered_user')
mpowered_password  = Variable.get('mpowered_password')
mpowered_tenant_table = "partner_schema.data_load_batch_details"
mpowered_resource_table = 'partner_schema.data_load_resource_details'
mpowered_affliate_table = 'partner_schema.affiliate'
mpowered_user_table = 'partner_schema.sample_user_plan'

#Ingestion Buckets
mapping_bucket_name = Variable.get('template_bucket_name')
mapping_config_key = "config/template/partner_config.json"

#Email
email_aws_access_key_id = Variable.get('email_aws_access_key_id')
email_aws_secret_access_key = Variable.get('email_aws_secret_access_key')
email_aws_region = Variable.get('email_aws_region')
sender ="noreply@mpoweredhealth.com"
ba_recipient = Variable.get("ba_recipients",deserialize_json=True)
dev_recipient = Variable.get("dev_recipient",deserialize_json=True)
client_recipient = {'emails':[]}

#Ingestion Endpoints
target_ingestion_endpoint = Variable.get('target_ingestion_endpoint')
target_ingestion_username = Variable.get('target_ingestion_username')
target_ingestion_password = Variable.get('target_ingestion_password')

#LOB Details
organization_name = Variable.get('organization_name')
affliate_name = Variable.get('affliate_name')
lob_name = Variable.get('lob_name')
schedule_type = Variable.get('schedule_type')

download_path = '/home/airflow/'

def create_name(organization_name,affliate_name,lob_name):
    name = list(organization_name.lower() + '-' + affliate_name.lower() + '-' + lob_name.lower())
    for i,chr in enumerate(name):
        if not re.match('[A-Za-z0-9-.]',chr):
            name[i] = "-"
    return ''.join(name)

name = create_name(organization_name,affliate_name,lob_name)
bucket_name = name + '-data-ingestion-qa'

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

default_args = {
    "owner": "Airflow"
    }

columns_in_use = []
start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id='partner_env_creation',
    schedule_interval = None,
    catchup = False,
    start_date = start_date,
    default_args=default_args
)

def send_mail(type,failure_step,results):
    CONFIGURATION_SET = "ConfigSet"
    CHARSET = "UTF-8"
    if (type=="failure"):
        subject = "Partner Env Creation Failure"
        if failure_step=="ingest_to_smilecdr":
            results_list = []
            for result in results['failure']:
                results_list.append(result +"<br>")
            results_string = ''.join(results_list)
            body = """<html><head>Ingestion Results</head>
                <body> Number of failed ingestions = """+str(len(results['failure']))+""" out of 
                """+str(len(results['success'])+len(results['failure']))+"""<br>"""+results_string+""" </body></html>"""
        else:
            body = """<html> <body> Hi, Ingestion failed at """+failure_step+""". </body></html>"""
    elif(type=="success"):
        subject = "Partner Env Creation Success"
        body = """<html> <body><p> Hi, Partner Bucket - """+bucket_name+""" has been created.</p>
        <p>Please fill the mapping files of each resources, validation if needed.</p>
        <p>Contact the developer for creating the code.</body></html>"""
    mail_client = boto3.client('ses',aws_access_key_id=email_aws_access_key_id, 
            aws_secret_access_key=email_aws_secret_access_key,region_name=email_aws_region)
    mail_client.send_email(Destination={'ToAddresses':ba_recipient['emails']+dev_recipient['emails']+client_recipient['emails']},
    Message={'Body':{'Html':{'Charset':CHARSET,'Data':body},},'Subject':{'Charset':CHARSET,'Data':subject},},Source=sender,)

def config_file_creation(ds,**kwargs):
    conn = psycopg2.connect(
    "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
    "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute(
        "select a.organization_id,a.affliate_id,a.lob_id,a.organization_name,a.affliate_name,a.lob_name,a.batch_start_date,a.batch_schedule_time,a.status,a.lob_updated,c.tenant_name,b.organization_id,b.affliate_id,b.lob_id,b.resource,b.file_type,b.resource_start_date,b.schedule_type,b.schedule_time,b.frequency,b.resource_sequence,b.last_run,b.status,a.id,b.id,b.batch_id from "+mpowered_tenant_table+" a join "+mpowered_resource_table+" b on a.id = b.batch_id join "+mpowered_affliate_table+" c on a.affliate_id = c.id where a.organization_name = '"+organization_name+"' and a.affliate_name = '"+affliate_name+"' and a.lob_name = '"+lob_name+"' order by b.resource_sequence asc")
    # Fetching all row from the table
    result = cursor.fetchall();
    dict = {}
    for i in range(len(result)):
        organization_id = result[i][0]
        affliate_id = result[i][1]
        lob_id = result[i][2]
        batch_id = result[i][25]
        key = str(organization_id) + '_' + str(affliate_id) + '_' + str(lob_id) + '_' + str(batch_id)
        if key not in dict:
            dict[key] = {}
            dict[key]['organization_id'] = result[i][0]
            dict[key]['affliate_id'] = result[i][1]
            dict[key]['lob_id'] = result[i][2]
            dict[key]['batch_id'] = result[i][25]
            dict[key]['organization_name'] = result[i][3]
            dict[key]['affliate_name'] = result[i][4]
            dict[key]['lob_name'] = result[i][5]
            dict[key]['batch_start_date'] = result[i][6]
            dict[key]['batch_schedule_time'] = result[i][7]
            dict[key]['tenant_name'] = result[i][10]
            if result[i][17] == 'file sensed':
                dict[key]['schedule_type'] = 'filesensed'
            elif result[i][17] == 'time based':
                dict[key]['schedule_type'] = 'time-based'
            else:
                dict[key]['schedule_type'] = result[i][17]
            dict[key]['resources'] = []
            dict[key]['file_type'] = {}
            dict[key]['resource_start_date'] = {}
            dict[key]['schedule_time'] = {}
            dict[key]['frequency'] = {}
            dict[key]['ba_email'] = []
            dict[key]['partner_email'] = []
        dict[key]['resources'].append(result[i][14])
        dict[key]['file_type'][result[i][14]] = result[i][15]
        dict[key]['resource_start_date'][result[i][14]] = result[i][16]
        dict[key]['schedule_time'][result[i][14]] = result[i][18]
        dict[key]['frequency'][result[i][14]] = result[i][19]

    config_json = {}
    for key in dict:
        config_json['organization_id'] = str(dict[key]['organization_id'])
        config_json['affliate_id'] = str(dict[key]['affliate_id'])
        config_json['lob_id'] = str(dict[key]['lob_id'])
        config_json['batch_id'] = str(dict[key]['batch_id'])
        config_json['organization_name'] = dict[key]['organization_name']
        config_json['affliate_name'] = dict[key]['affliate_name']
        config_json['lob_name'] = dict[key]['lob_name']
        if dict[key]['batch_start_date']:
            config_json['batch_start_date'] = str(dict[key]['batch_start_date'])
        if dict[key]['batch_schedule_time']:
            config_json['batch_schedule_time'] = str(dict[key]['batch_schedule_time'])
        config_json['tenant_name'] = str(dict[key]['tenant_name'])
        config_json['resources'] = dict[key]['resources']
        config_json['file_type'] = dict[key]['file_type']
        config_json['resource_start_date'] = dict[key]['resource_start_date']
        config_json['schedule_type'] = str(dict[key]['schedule_type'])
        config_json['schedule_time'] = dict[key]['schedule_time']
        config_json['frequency'] = dict[key]['frequency']
    with open(download_path+'partner_config.json', 'w') as f:
        json.dump(config_json, f)
    # s3_client.put_object(Bucket=mapping_bucket_name,Key=('config/'+name+'-'+schedule_type+'/'))
    s3_resource.Bucket(mapping_bucket_name).upload_file(download_path+'partner_config.json','config/'+name+'-'+schedule_type+'/partner_config.json')

def bucket_creation(ds,**kwargs):
    input_file = s3_client.read_file = s3_client.get_object(Bucket=mapping_bucket_name,Key='config/'+name+'-'+schedule_type+'/partner_config.json')
    config_json = json.load(input_file['Body'])
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name})
        bucket_versioning = s3_resource.BucketVersioning(bucket_name)
        bucket_versioning.enable()
        response_public = s3_client.put_public_access_block(Bucket=bucket_name,
            PublicAccessBlockConfiguration={'BlockPublicAcls': True,'IgnorePublicAcls': True,'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True},)
    except:
        print("Bucket Already Created")
    folders = ["archive/","concatenated/","great_expectations/","great_expectations/data_docs/",
    "great_expectations/expectations/","great_expectations/processed/","great_expectations/validation_fail/",
    "great_expectations/validation_success/","great_expectations/validation_result/",
    "mapped/","mapping/","processed/","raw/","unprocessed/","consent_config/","dags/"]
    no_resource_folder = ["consent_config/","dags/"]
    for folder in folders:
        s3_client.put_object(Bucket=bucket_name,Key=(folder))
        if folder not in no_resource_folder:
            for resource in config_json['resources']:
                s3_client.put_object(Bucket=bucket_name,Key=(folder+resource+'/'))
                if config_json['file_type'][resource] == 'xml' and folder == 'raw/':
                    s3_client.put_object(Bucket=bucket_name,Key=(folder+'CCDA_'+resource+'/'))
    for resource in config_json['resources']:
        s3_resource.meta.client.copy({'Bucket':mapping_bucket_name,'Key':'templates/'+resource+'/'+resource+'_Mapping.csv'},bucket_name,'mapping/'+resource+'/'+resource+'_Mapping.csv')
        s3_resource.meta.client.copy({'Bucket':mapping_bucket_name,'Key':'templates/'+resource+'/'+resource+'_Id_search.csv'},bucket_name,'mapping/'+resource+'/'+resource+'_Id_search.csv')
        s3_resource.meta.client.copy({'Bucket':mapping_bucket_name,'Key':'templates/'+resource+'/'+resource+'_fhir_values.csv'},bucket_name,'mapping/'+resource+'/'+resource+'_fhir_values.csv')
        s3_resource.meta.client.copy({'Bucket':mapping_bucket_name,'Key':'config/template/validation_template.csv'},bucket_name,'mapping/'+resource+'/'+resource+'_Validation.csv')
    conn = psycopg2.connect(
    "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
    "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute(
    "update "+mpowered_tenant_table+" set status = 'pending' where organization_id = "+config_json['organization_id']+" and affliate_id = "+config_json['affliate_id'] + " and lob_id = "+config_json['lob_id'] + " and id = "+config_json['batch_id'])
    conn.commit()
    cursor.close()

def resource_dag_creation(ds,**kwargs):
    os.system("aws configure set aws_access_key_id "+aws_access_key_id)
    os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
    resource_dag_template_key = "config/template/data-ingestion-airflow-template.py"
    s3_client.download_file(mapping_bucket_name,resource_dag_template_key,download_path+'template.py')
    input_file = s3_client.read_file = s3_client.get_object(Bucket=mapping_bucket_name,Key='config/'+name+'-'+schedule_type+'/partner_config.json')
    template = open(download_path+'template.py')
    config_json = json.load(input_file['Body'])
    resources = config_json['resources']
    tenant_name = config_json['tenant_name']
    file_type = config_json['file_type']
    batch_start_date = config_json['batch_start_date']
    batch_schedule_time = config_json['batch_schedule_time']
    frequency = config_json['frequency']
    validation_lines = []
    for i in range(len(resources)):
        if i==len(resources)-1:
            validation_lines.extend(['\t\t\t\t\t\t\tresources[',str(i),']: {\n','\t\t\t\t\t\t\t\t"prefix": "concatenated/"+resources[',str(i),']+"/"',',\n','\t\t\t\t\t\t\t\t"regex_filter": ".*"\n','\t\t\t\t\t\t\t}\n'])
        else:
            validation_lines.extend(['\t\t\t\t\t\t\tresources[',str(i),']: {\n','\t\t\t\t\t\t\t\t"prefix": "concatenated/"+resources[',str(i),']+"/",\n','\t\t\t\t\t\t\t\t"regex_filter": ".*"\n','\t\t\t\t\t\t\t},\n'])
    
    for resource in resources:
        resource_dag_key = download_path+name+'_'+resource+'_Bulk_Dag.py'
        with open(resource_dag_key,'w') as file:
            with open(download_path+'template.py') as template:
                for line in template:
                    line = line.replace("organization_name = None", "organization_name = '"+organization_name+"'")
                    line = line.replace("affliate_name = None", "affliate_name = '"+affliate_name+"'")
                    line = line.replace("lob_name = None", "lob_name ='"+lob_name+"'")
                    line = line.replace("tenant_name = None","tenant_name = '"+tenant_name+"'")
                    line = line.replace("resources = None","resources = "+str(resources))
                    line = line.replace("actualresource = None", "actualresource = '"+str(resource)+"'")                   
                    line = line.replace("name = None", "name = '"+name+"'")
                    line = line.replace("file_type = None","file_type = '"+str(file_type[resource])+"'")
                    line = line.replace("schedule_type = None","schedule_type = '"+str(schedule_type)+"'")
                    if schedule_type == 'manual':
                        line = line.replace("start_date = None","start_date = airflow.utils.dates.days_ago(1)")
                    elif schedule_type == 'filesensed':
                        mm,dd,yyyy = str(resource_start_date[resource]).split('/')
                        today = datetime.date.today()
                        now = datetime.datetime.now()
                        start_date = datetime.date(int(yyyy),int(mm),int(dd))
                        if today>start_date:
                            mm = today.month
                            dd = today.day
                            yyyy = today.year
                            hr = now.hour
                            min = now.minute
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},{},{})".format(int(yyyy),int(mm),int(dd),int(hr),int(min)))
                        else:
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},0,0)".format(int(yyyy),int(mm),int(dd)))
                        line = line.replace("schedule_interval = None","schedule_interval = '0 * * * *'")
                    elif schedule_type == 'time-based':
                        mm,dd,yyyy = str(resource_start_date[resource]).split('/')
                        today = datetime.date.today()
                        start_date = datetime.date(int(yyyy),int(mm),int(dd))
                        if today>start_date:
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},0,0)".format(int(today.year),int(today.month),int(today.day)))
                        else:
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},0,0)".format(int(yyyy),int(mm),int(dd)))
                        hr,min = str(schedule_time[resource]).split(':')
                        if frequency[resource] == 'hourly':
                            cron = '{} * * * *'.format(min)
                        elif frequency[resource] == 'daily':
                            cron = '{} {} * * *'.format(min,hr)
                        elif frequency[resource] == 'weekly':
                            cron = '{} {} * * {}'.format(min,hr,today.isoweekday())
                        elif frequency[resource] == 'monthly':
                            cron = '{} {} {} * *'.format(min,hr,dd)
                        elif frequency[resource] == 'yearly':
                            cron = '{} {} {} {} *'.format(min,hr,dd,mm)
                        line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                    if line.strip()=='validation_replace':
                        file.writelines(validation_lines)
                    else:
                        file.write(line)
        os.system("aws s3 cp "+resource_dag_key+" s3://"+bucket_name+"/dags/")
        os.system("rm "+resource_dag_key)
    if schedule_type == 'manual':
        os.system("aws configure set aws_access_key_id "+aws_access_key_id)
        os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
        resource_dag_template_key = "config/template/data-ingestion-trigger-template.py"
        s3_client.download_file(mapping_bucket_name,resource_dag_template_key,download_path+'template.py')
        resource_dag_key = download_path+name+'_trigger_Bulk_Dag.py'

        lines = []
        lines.extend(["""\n\tset_aws_key = BashOperator(task_id='set_aws_key',bash_command='aws configure set aws_access_key_id '+aws_access_key_id,dag=dag)"""])
        lines.extend(["""\n\tset_aws_secret = BashOperator(task_id='set_aws_secret',bash_command='aws configure set aws_secret_access_key '+aws_secret_access_key,dag=dag)"""])
        for resource in resources:
            lines.extend(['\n\n\ttrigger_target_',resource,' = TriggerDagRunOperator(',
            '\n\ttask_id="trigger_target_',resource,'",',
            '\n\ttrigger_dag_id=name+"_',resource,'_Bulk_Dag",',
            '\n\texecution_date="{{ ds }}",',
            '\n\treset_dag_run=True,',
            '\n\twait_for_completion=True,',
            '\n\tpoke_interval=10',
            '\n\t)'])

        lines.extend(['\n\n\tset_aws_key >> set_aws_secret'])
        for resource in resources:
            lines.extend(['>> trigger_target_',resource])
        with open(download_path+'template.py') as template:
            with open(resource_dag_key,'w') as file:
                for line in template:
                    line = line.replace("organization_name = None", "organization_name = '"+organization_name+"'")
                    line = line.replace("affliate_name = None", "affliate_name = '"+affliate_name+"'")
                    line = line.replace("lob_name = None", "lob_name = '"+lob_name+"'")
                    line = line.replace("tenant_name = None","tenant_name = '"+tenant_name+"'")
                    line = line.replace("resources = None","resources = "+str(resources))
                    line = line.replace("actualresource = None", "actualresource = '"+str(resource)+"'")            
                    line = line.replace("name = None", "name = '"+name+"'")
                    if schedule_type == 'manual':
                        mm,dd,yyyy = str(batch_start_date).split('/')
                        today = datetime.date.today()
                        start_date = datetime.date(int(yyyy),int(mm),int(dd))
                        if today>start_date:
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},0,0)".format(int(today.year),int(today.month),int(today.day)))
                        else:
                            line = line.replace("start_date = None","start_date = datetime.datetime({},{},{},0,0)".format(int(yyyy),int(mm),int(dd)))
                        hr,min = str(batch_schedule_time).split(':')
                        if frequency[resource] == 'hourly':
                            cron = '{} * * * *'.format(min)
                            line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                        elif frequency[resource] == 'daily':
                            cron = '{} {} * * *'.format(min,hr)
                            line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                        elif frequency[resource] == 'weekly':
                            cron = '{} {} * * {}'.format(min,hr,today.isoweekday())
                            line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                        elif frequency[resource] == 'monthly':
                            cron = '{} {} {} * *'.format(min,hr,dd)
                            line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                        elif frequency[resource] == 'yearly':
                            cron = '{} {} {} {} *'.format(min,hr,dd,mm)
                            line = line.replace("schedule_interval = None","schedule_interval = '"+cron+"'")
                    file.write(line)
                file.writelines(lines)
        os.system("aws s3 cp "+resource_dag_key+" s3://"+bucket_name+"/dags/")
        os.system("rm "+resource_dag_key)

def update_postgres(ds,**kwargs):
    input_file = s3_client.read_file = s3_client.get_object(Bucket=mapping_bucket_name,Key='config/'+name+'-'+schedule_type+'/partner_config.json')
    config_json = json.load(input_file['Body'])
    batch_id = config_json['batch_id']
    conn = psycopg2.connect(
    "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
    "password='"+mpowered_password+"'")
    cursor = conn.cursor()
    cursor.execute(
        "update "+mpowered_tenant_table+" set status = 'completed' where organization_name = '"+organization_name+"' and affliate_name = '" + affliate_name + "' and LOB_name = '" + lob_name +"' and id =" + str(batch_id))
    conn.commit()
    cursor.close()
    send_mail("success",None,None)


task_config_file_creation = PythonOperator(
    task_id='task_config_file_creation',
    python_callable=config_file_creation,
    provide_context=True,
    dag=dag)

task_bucket_creation = PythonOperator(
    task_id='task_bucket_creation',
    python_callable=bucket_creation,
    provide_context=True,
    dag=dag)

task_resource_dag_creation = PythonOperator(
    task_id='task_resource_dag_creation',
    python_callable=resource_dag_creation,
    provide_context=True,
    dag=dag)

task_update_postgres = PythonOperator(
    task_id='task_update_postgres',
    python_callable=update_postgres,
    provide_context=True,
    dag=dag)

task_config_file_creation >> task_bucket_creation >> task_resource_dag_creation >> task_update_postgres