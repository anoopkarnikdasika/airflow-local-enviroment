from email import message
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime
import boto3
from urllib.parse import urlparse
import pandas as pd
import json
import os
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import *
import botocore

import io
import psycopg2
from fhir.resources.allergyintolerance import AllergyIntolerance
from fhir.resources.identifier import Identifier
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.extension import Extension
from fhir.resources.reference import Reference
from fhir.resources.period import Period
from fhir.resources.backboneelement import BackboneElement
from fhir.resources.annotation import Annotation
import concurrent.futures
import airflow
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
email_aws_access_key_id = Variable.get("email_aws_access_key_id")
email_aws_secret_access_key = Variable.get("email_aws_secret_access_key")
email_aws_region = Variable.get("email_aws_region")
bucket_name = Variable.get("ehealth_bucket_name")
template_bucket_name = Variable.get("template_bucket_name")
sender = Variable.get("sender")
ba_recipient = Variable.get("ba_recipients",deserialize_json=True)
dev_recipient = Variable.get("dev_recipient",deserialize_json=True)
client_recipient = {'emails':[]}
mpowered_host = Variable.get("mpowered_host")
mpowered_port = Variable.get("mpowered_port")
mpowered_database = Variable.get("mpowered_database")
mpowered_user = Variable.get("mpowered_user")
mpowered_password  = Variable.get("mpowered_password")

organization_name = 'Scan ORG'
affliate_name = 'Scan'
lob_name ='Scan Mock Data'
tenant_name = '896_379'
resources = ['Practitioner','PractitionerRole','Patient', 'AllergyIntolerance', 'Condition', 'SocialHistory', 'DiagnosticReport', 'Procedure', 'Immunization', 'Medication', 'Vitals', 'Encounter', 'ExplanationOfBenefit', 'Coverage']
file_type = 'csv'
name = 'scan-org-scan-scan-mock-data'
schedule_type = 'manual'
bucket_name = name+'-data-ingestion-qa'

default_args = {
    "owner": "Airflow"
    }

columns_in_use = []
start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id=name+'_Trigger_Bulk_Dag',
    schedule_interval = None,
    catchup = False,
    start_date = start_date,
    default_args=default_args
)

task_ingest_resource_1 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[0]}',
    trigger_dag_id = f'{name}_{resources[0]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_2 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[1]}',
    trigger_dag_id = f'{name}_{resources[1]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_3 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[2]}',
    trigger_dag_id = f'{name}_{resources[2]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_4 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[3]}',
    trigger_dag_id = f'{name}_{resources[3]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_5 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[4]}',
    trigger_dag_id = f'{name}_{resources[4]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_6 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[5]}',
    trigger_dag_id = f'{name}_{resources[5]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_7 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[6]}',
    trigger_dag_id = f'{name}_{resources[6]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_8 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[7]}',
    trigger_dag_id = f'{name}_{resources[7]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_9 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[8]}',
    trigger_dag_id = f'{name}_{resources[8]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_10 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[9]}',
    trigger_dag_id = f'{name}_{resources[9]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_11 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[10]}',
    trigger_dag_id = f'{name}_{resources[10]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_12 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[11]}',
    trigger_dag_id = f'{name}_{resources[11]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_13 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[12]}',
    trigger_dag_id = f'{name}_{resources[12]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_14 = TriggerDagRunOperator(
    task_id=f'task_ingest_{resources[13]}',
    trigger_dag_id = f'{name}_{resources[13]}_Bulk_Dag',
    dag=dag)

task_ingest_resource_1 >> task_ingest_resource_2 >> task_ingest_resource_3 >> task_ingest_resource_4 >>task_ingest_resource_5 >> task_ingest_resource_6 >> task_ingest_resource_7 >> task_ingest_resource_8 >> task_ingest_resource_9 >> task_ingest_resource_10 >> task_ingest_resource_11 >> task_ingest_resource_12 >> task_ingest_resource_13 >> task_ingest_resource_14 