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
from fhir.resources.medicationstatement import MedicationStatement
from fhir.resources.medicationdispense import MedicationDispense
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
tenant_name = Variable.get('tenant_name')
resources = ['Patient', 'AllergyIntolerance', 'Condition', 'Observation', 'Encounter', 'Procedure', 'Immunization', 'Encounter', 'Medication', 'MedicationRequest', 'Claim', 'ExplanationOfBenefit', 'Coverage', 'InsurancePlan', 'Observation']
actualresource = 'Medication'
file_type = 'csv'
name = 'scan-org-scan-scan-mock-data'
schedule_type = 'manual'
bucket_name = name+'-data-ingestion-qa'

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

def send_mail(type,failure_step,results,file_name):
    CONFIGURATION_SET = "ConfigSet"
    CHARSET = "UTF-8"
    if (type=="failure"):
        subject = "Failure Ingestion Mail"
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
        subject = name+" "+actualresource+" Ingestion Mail"
        body = """<html> <body> <p>"""+name+""" """+actualresource+""" Ingestion completed for file ="""+file_name+""" </p></body></html>"""
    elif(type=="validation"):
        subject = "Validation "+failure_step+ " Mail"
        key = list(results['run_results'].keys())[0]
        validation_results = results['run_results'][key]['validation_result']
        results_list = [] 
        for i in range(len(validation_results['results'])):
            results_list.append('<tr>')
            results_list.append('<th>'+ str(validation_results['results'][i]['expectation_config']['kwargs']['column']) + '</th>')
            results_list.append('<th>'+ str(validation_results['results'][i]['expectation_config']['expectation_type']) + '</th>')
            results_list.append('<th>'+ str(validation_results['results'][i]['success']) + '</th>')
            results_list.append('<th>'+ str(validation_results['results'][i]['result']['element_count']) + '</th>')
            results_list.append('<th>'+ str(validation_results['results'][i]['result']['unexpected_percent']) + '</th>')
            results_list.append('</tr>')
        results_string = ''.join(results_list)
        body = """<html><head>Validation Results</head>
            <style>table, th, td {border:1px solid black;}</style>
            <body><table><tr>
                <th> Column  name </th>
                <th> Expectation name </th>
                <th> Success </th>
                <th> Element Count </th>
                <th> Unexpected Percent </th>
            </tr> """+results_string+""" </table></body></html>"""
    mail_client = boto3.client('ses',aws_access_key_id=email_aws_access_key_id, 
            aws_secret_access_key=email_aws_secret_access_key,region_name=email_aws_region)
    mail_client.send_email(Destination={'ToAddresses':ba_recipient['emails']+dev_recipient['emails']+client_recipient['emails']},
    Message={'Body':{'Html':{'Charset':CHARSET,'Data':body},},'Subject':{'Charset':CHARSET,'Data':subject},},Source=sender,)

def concatenate(ds, **kwargs):
    my_bucket = s3_resource.Bucket(bucket_name)
    files = my_bucket.objects.filter(Prefix="raw/"+actualresource+"/")
    files = [obj.key for obj in sorted(files,key=lambda x: x.last_modified)]
    file_dict = []
    for key in files:
        if ".csv" in key:
            file_name = key.split('/')[-1][:-4]
            read_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=key)
            csv_file = pd.read_csv(io.StringIO(read_file['Body'].read().decode('ISO-8859-1')),dtype=object,encoding='ISO-8859-1')
            if len(csv_file) == 0:
                os.system("aws s3 mv s3://"+bucket_name+"/"+key+" s3://"+bucket_name+"/archive/"+actualresource+"/"+file_name+"_"+str(time.time())+".csv")
            if len(csv_file)>0:
                csv_file['file_name'] = file_name
                file_dict.append(csv_file)
                # os.system("aws s3 mv s3://"+bucket_name+"/"+key+" s3://"+bucket_name+"/archive/"+actualresource+"/"+file_name+"_"+str(time.time())+".csv")
    if len(file_dict) == 0:
        message = "input csv file not present or empty in the raw location"
        # send_mail("failure","concatenation",None)
    input_df = pd.concat(file_dict)
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'concatenated/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

# def validation(ds, **kwargs):
#     os.system("aws configure set aws_access_key_id "+aws_access_key_id)
#     os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
#     template_key = "config/template/validation_template.json"
#     template = s3_resource.Object(template_bucket_name,template_key).get()['Body'].read().decode('utf-8')
#     input_key = "concatenated/"+actualresource+"/"+actualresource+".csv"
#     input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
#     input_df = pd.read_csv(input_file['Body'])
#     validation_key = "mapping/" + actualresource + "/" + actualresource + "_Validation.csv"
#     read_validation_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name, Key=validation_key)
#     validation = pd.read_csv(read_validation_file['Body'])
#     template_json = json.loads(template)
#     template_json['expectation_suite_name'] = actualresource+".validation"
#     template_json['meta']['citations'][0]['batch_kwargs']['data_asset_name'] = actualresource
#     template_json['meta']['citations'][0]['batch_kwargs']['s3'] = "s3://"+bucket_name+"/concatenated/"+actualresource+"/"+actualresource+".csv"
#     for column in validation.columns:
#         for i in range(len(validation)):
#             if not pd.isna(validation[column].iloc[i]):
#                 validation_dict = {}
#                 validation_dict["expectation_type"] = column
#                 validation_dict["kwargs"] = {}
#                 validation_dict["kwargs"]["column"] = validation['column'].iloc[i]
#                 validation_dict["meta"] = {}
#                 if validation_dict['expectation_type'] !='column':
#                     template_json['expectations'].append(validation_dict)
#     template_json_object = json.dumps(template_json,indent=4)
#     with open("validation.json","w") as outfile:
#         outfile.write(template_json_object)
#     s3_resource.Bucket(bucket_name).upload_file("validation.json","great_expectations/expectations/"+actualresource+"/validation.json")
#     # os.system("rm "+"/home/ec2-user/python_ingestion_codes/ehealth/validation.json")
#     project_config = DataContextConfig(
#         config_version=2,
#         plugins_directory=None,
#         config_variables_file_path=None,
#         datasources={
#             "pandas_s3": DatasourceConfig(
#                 class_name="PandasDatasource",
#                 batch_kwargs_generators={
#                     "pandas_s3_generator": {
#                         "class_name": "S3GlobReaderBatchKwargsGenerator",
#                         "bucket": bucket_name,
#                         "assets": {
#                             validation_replace
#                         }
#                     }
#                 },
#                 module_name="great_expectations.datasource",
#                 data_asset_type={
#                     "class_name": "PandasDataset",
#                     "module_name": "great_expectations.dataset"
#                 }
#             )
#         },
#         store_backend_defaults=S3StoreBackendDefaults(default_bucket_name=bucket_name),
#         stores={
#             "expectations_S3_store": {
#                 "class_name": "ExpectationsStore",
#                 "store_backend": {
#                     "class_name": "TupleS3StoreBackend",
#                     "bucket": bucket_name,
#                     "prefix": "great_expectations/expectations/" + actualresource,
#                 },
#             },
#             "validations_S3_store": {
#                 "class_name": "ValidationsStore",
#                 "store_backend": {
#                     "class_name": "TupleS3StoreBackend",
#                     "bucket": bucket_name,
#                     "prefix": "great_expectations/validation_result/" + actualresource,
#                 },
#             },
#             "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
#         },
#         expectations_store_name="expectations_S3_store",
#         validations_store_name="validations_S3_store",
#         evaluation_parameter_store_name="evaluation_parameter_store",
#         data_docs_sites={
#             "s3_site": {
#                 "class_name": "SiteBuilder",
#                 "store_backend": {
#                     "class_name": "TupleS3StoreBackend",
#                     "bucket": bucket_name,
#                     "prefix": "great_expectations/data_docs/" + actualresource
#                 },
#                 "site_index_builder": {
#                     "class_name": "DefaultSiteIndexBuilder",
#                     "show_cta_footer": True,
#                 },
#             }
#         },
#         validation_operators={
#             "action_list_operator": {
#                 "class_name": "ActionListValidationOperator",
#                 "action_list": [
#                     {
#                         "name": "update_data_docs",
#                         "action": {"class_name": "UpdateDataDocsAction"},
#                     },
#                     {
#                         "name": "store_validation_result",
#                         "action": {"class_name": "StoreValidationResultAction"},
#                     },
#                     {
#                         "name": "store_evaluation_params",
#                         "action": {"class_name": "StoreEvaluationParametersAction"},
#                     },
#                 ],
#             }
#         },
#         anonymous_usage_statistics={
#             "enabled": True
#         }
#     )
#     run_id={"run_name": actualresource, "run_time": datetime.datetime.now(datetime.timezone.utc)}
#     context = BaseDataContext(project_config=project_config)
#     expectation_suite_name = "validation"
#     suite = context.get_expectation_suite(expectation_suite_name)
#     batch_kwargs = context.build_batch_kwargs(data_asset_name=actualresource,
#                                             batch_kwargs_generator='pandas_s3_generator', datasource='pandas_s3')
#     batch = context.get_batch(batch_kwargs, suite)
#     results = context.run_validation_operator("action_list_operator",assets_to_validate=[batch],
#     result_format='COMPLETE',run_id=run_id)
#     csv_buffer = StringIO()
#     input_df.to_csv(csv_buffer, index=False)
#     if not results["success"]:
#         send_mail("validation","Failure",results)
#         s3_resource.Object(bucket_name,'great_expectations/validation_fail/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())
#     else:
#         # send_mail("validation","Success",results)
#         s3_resource.Object(bucket_name,'great_expectations/validation_success/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())


def standardization_mapped(ds, **kwargs):
    columns_in_use = []
    input_key = "concatenated/"+actualresource+"/"+actualresource+".csv"
    # input_key = "great_expectations/validation_success/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    input_df['id'] = input_df.index
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'mapped/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

def ingest_to_smilecdr(ds, **kwargs):
    input_key = "mapped/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'],keep_default_na=False,dtype={
        'Code':str, 'Patient_Id': str, 'No_of_pills':str
    })
    ingestion_success = []
    ingestion_failure = []
    ingested_ids = []
    def ingest(i):
        row = input_df.iloc[i]
        print(f"Current row of data = {row}")
        medication = {}
        medication['resourceType'] = 'MedicationRequest'
        medication['identifier']= list()
        medication['identifier'].append({'system':'data_source','value':'Scan'})
        if row['Status']!='':
            medication['status'] = row['Status']
        if row['Name']!='':
            codeableConcept = {}
            codeableConcept['text'] = row['Name']
            codeableConcept['coding'] = list()
            codeableConcept['coding'].append({'display':row['Name'],'code':row['Code']})
            medication['medicationCodeableConcept'] = codeableConcept
        if row['Frequency']!='' or row['Dosage']!='' or row['No_of_pills']!='':
            dosage = {}
            if row['Dosage']!='':
                dosage['text'] = row['Dosage']
            if row['Dosage']!='' or row['No_of_pills']!='':
                dosage['timing'] = {}
                if row['Frequency']!='':
                    dosage['timing']['code'] = {'text':row['Frequency']}
                if row['No_of_pills']!='' or row['Time']!='':
                    dosage['timing']['repeat'] = {}
                    if row['No_of_pills']!='':
                        dosage['timing']['repeat']['frequency'] =str(row['No_of_pills'])
                    if row['Time']!='':
                        dosage['timing']['repeat']['timeOfDay'] = [row['Time']]
                    if row['Start_Date']!='' or row['End_Date'] != '':
                        dosage['timing']['repeat']['boundsPeriod'] = {}
                        if row['Start_Date']!='':
                            dosage['timing']['repeat']['boundsPeriod']['start'] = str(row['Start_Date'])
                        if row['End_Date']!='':
                            dosage['timing']['repeat']['boundsPeriod']['end'] = str(row['End_Date'])  
            medication['dosageInstruction'] = list()
            medication['dosageInstruction'].append(dosage)
        if row['Start_Date']!='':
           medication['authoredOn'] = str(row['Start_Date'])
        if row['Recorded_by']!='':
            medication['performer'] = {'display':row['Recorded_by']}
        if row['Requested_by']!='':
            medication['requester'] = {'display':row['Requested_by']}
        if row['Prescriber']!='':
            medication['recorder'] = {'display':row['Prescriber']}
        if row['Note']!='':
            medication['note'] = list()
            annotation = {}
            annotation['text'] = row['Note']
            medication['note'].append(annotation)
        print(f"patient url = {target_ingestion_endpoint}{tenant_name}/Patient?identifier={str(int(float(row['Patient_Id'])))}")
        patient_response = requests.get(target_ingestion_endpoint+tenant_name+'/Patient?identifier='+str(int(float(row['Patient_Id']))),
                auth=(target_ingestion_username , target_ingestion_password)).json()
        print(patient_response)
        medication['subject']= {'reference':'Patient/'+patient_response['entry'][0]['resource']['id']}
        print(json.loads(json.dumps(medication)))
        response = requests.post(target_ingestion_endpoint+tenant_name+'/MedicationRequest',
                json=json.loads(json.dumps(medication)),
                auth=(target_ingestion_username , target_ingestion_password)).json()
        print(response)
        print(response['id'])
    # with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    #     print(input_df)
    #     res = [executor.submit(ingest,i) for i in range(len(input_df))]
    #     concurrent.futures.wait(res)
    #     print("All tasks are done")
    #     results = {}
    #     results['failure'] = ingestion_failure
    #     results['success'] = ingestion_success
    #     if len(results['failure'])>0:
    #         send_mail("failure","ingest_to_smilecdr",results)
    for i in range(len(input_df)):
        ingest(i)
    input_key = "concatenated/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    input_df['id'] = input_df.index
    success_df = input_df[input_df['id'].isin(ingested_ids)]
    failure_df = input_df[~input_df['id'].isin(ingested_ids)]
    if(len(success_df)>0):
        csv_buffer = StringIO()
        success_df.to_csv(csv_buffer, index=False)
        s3_resource.Object(bucket_name,'processed/'+actualresource+'/'+actualresource+'_'+str(time.time())+'.csv').put(Body=csv_buffer.getvalue())
    if(len(failure_df)>0):
        csv_buffer = StringIO()
        failure_df.to_csv(csv_buffer, index=False)
        s3_resource.Object(bucket_name,'unprocessed/'+actualresource+'/'+actualresource+'_'+str(time.time())+'.csv').put(Body=csv_buffer.getvalue())
    os.system("aws s3 rm s3://"+bucket_name+'/concatenated/'+actualresource+'/'+actualresource+'.csv')
    os.system("aws s3 rm s3://"+bucket_name+'/great_expectations/validation_success/'+actualresource+'/'+actualresource+'.csv')
    os.system("aws s3 rm s3://"+bucket_name+'/mapped/'+actualresource+'/'+actualresource+'.csv')
    my_bucket = s3_resource.Bucket(bucket_name)
    files = my_bucket.objects.filter(Prefix="archive/")
    files = [obj.key for obj in sorted(files,key=lambda x: x.last_modified)]
    file_name = files[0]
    send_mail("success",None,None,file_name)

# def update_postgres(ds, **kwargs):
#     time = str(date.today())
#     conn = psycopg2.connect(
#     "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
#     "password='"+mpowered_password+"'")
#     cursor = conn.cursor()
#     cursor.execute(
#         "update "+mpowered_resource_table+" set status = 'ingested',last_run='"+time+"' where organization_id = '"+organization_id+"' and affliate_id = '" + affliate_id + "' and lob_id = '" + lob_id+"' and resource='Patient'")
#     conn.commit()
#     cursor.close()

task_concatenate = PythonOperator(
    task_id='task_concatenate',
    python_callable=concatenate,
    provide_context=True,
    dag=dag)

# task_validation = PythonOperator(
#     task_id='task_validation',
#     python_callable=validation,
#     provide_context=True,
#     dag=dag)

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

# task_update_postgres = PythonOperator(
#     task_id='task_update_postgres',
#     python_callable=update_postgres,
#     provide_context=True,
#     dag=dag)

task_concatenate  >> task_standardization_mapped >> task_ingest_to_smilecdr
