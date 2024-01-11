from aifc import Aifc_read
from email import message
import requests
from requests.auth import HTTPBasicAuth
import time
import airflow
import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import Variable
import boto3
from urllib.parse import urlparse
import pandas as pd
import json
import os 
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import *
import botocore

import io
import smtplib, ssl
from fhir.resources.coverage import Coverage
from fhir.resources.identifier import Identifier
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.insuranceplan import InsurancePlan
from fhir.resources.extension import Extension
from fhir.resources.reference import Reference
from fhir.resources.period import Period
from fhir.resources.backboneelement import BackboneElement
import concurrent.futures
import psycopg2

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
email_aws_access_key_id = Variable.get("email_aws_access_key_id")
email_aws_secret_access_key = Variable.get("email_aws_secret_access_key")
email_aws_region = Variable.get("email_aws_region")
bucket_name = Variable.get("ehealth_bucket_name")
template_bucket_name = Variable.get("template_bucket_name")
target_ingestion_endpoint = Variable.get("target_ingestion_endpoint")
target_ingestion_username = Variable.get("target_ingestion_username")
target_ingestion_password = Variable.get("target_ingestion_password")
sender = Variable.get("sender")
ba_recipient = Variable.get("ehealth_ba_recipients",deserialize_json=True)
dev_recipient = Variable.get("dev_recipient",deserialize_json=True)
client_recipient = Variable.get("ehealth_client_recipients",deserialize_json=True)
mpowered_host = Variable.get("mpowered_host")
mpowered_port = Variable.get("mpowered_port")
mpowered_database = Variable.get("mpowered_database")
mpowered_user = Variable.get("mpowered_user")
mpowered_password  = Variable.get("mpowered_password")

organization_name = 'Ehealth'
affliate_name = 'affiliate1'
lob_name ='lob1'
tenant_name = 'DEFAULT'
resources = ['Coverage', 'InsurancePlan']
actualresource = 'Coverage'
file_type = 'csv'
name = 'ehealth-affiliate1-lob1'


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

def send_mail(type,failure_step,results):
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
        subject = "Complete Ehealth Ingestion Mail"
        body = """<html> <body> <p>Hi, </p> <p>Ehealth Coverage Ingestion completed.</p></body></html>"""
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
    os.system('aws configure set aws_access_key_id '+aws_access_key_id)
    os.system('aws configure set aws_secret_access_key '+aws_secret_access_key)
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
                os.system("aws s3 mv s3://"+bucket_name+"/"+key+" s3://"+bucket_name+"/archive/"+actualresource+"/"+file_name+".csv")
            if len(csv_file)>0:
                csv_file['file_name'] = file_name
                file_dict.append(csv_file)
                os.system("aws s3 mv s3://"+bucket_name+"/"+key+" s3://"+bucket_name+"/archive/"+actualresource+"/"+file_name+".csv")
    if len(file_dict) == 0:
        message = "input csv file not present or empty in the raw location"
        raise AirflowException(message)
    input_df = pd.concat(file_dict)
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'concatenated/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

def validation(ds, **kwargs):
    os.system("aws configure set aws_access_key_id "+aws_access_key_id)
    os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
    template_key = "config/template/validation_template.json"
    template = s3_resource.Object(template_bucket_name,template_key).get()['Body'].read().decode('utf-8')
    input_key = "concatenated/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    validation_key = "mapping/" + actualresource + "/" + actualresource + "_Validation.csv"
    read_validation_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name, Key=validation_key)
    validation = pd.read_csv(read_validation_file['Body'])
    template_json = json.loads(template)
    template_json['expectation_suite_name'] = actualresource+".validation"
    template_json['meta']['citations'][0]['batch_kwargs']['data_asset_name'] = actualresource
    template_json['meta']['citations'][0]['batch_kwargs']['s3'] = "s3://"+bucket_name+"/concatenated/"+actualresource+"/"+actualresource+".csv"
    for column in validation.columns:
        for i in range(len(validation)):
            if not pd.isna(validation[column].iloc[i]):
                validation_dict = {}
                validation_dict["expectation_type"] = column
                validation_dict["kwargs"] = {}
                validation_dict["kwargs"]["column"] = validation['column'].iloc[i]
                validation_dict["meta"] = {}
                if validation_dict['expectation_type'] !='column':
                    template_json['expectations'].append(validation_dict)
    template_json_object = json.dumps(template_json,indent=4)
    with open("validation.json","w") as outfile:
        outfile.write(template_json_object)
    s3_resource.Bucket(bucket_name).upload_file("validation.json","great_expectations/expectations/"+actualresource+"/validation.json")
    # os.system("rm "+"/home/ec2-user/python_ingestion_codes/ehealth/validation.json")
    project_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        datasources={
            "pandas_s3": DatasourceConfig(
                class_name="PandasDatasource",
                batch_kwargs_generators={
                    "pandas_s3_generator": {
                        "class_name": "S3GlobReaderBatchKwargsGenerator",
                        "bucket": bucket_name,
                        "assets": {
							resources[0]: {
								"prefix": "concatenated/"+resources[0]+"/",
								"regex_filter": ".*"
							},
							resources[1]: {
								"prefix": "concatenated/"+resources[1]+"/",
								"regex_filter": ".*"
							}
                        }
                    }
                },
                module_name="great_expectations.datasource",
                data_asset_type={
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset"
                }
            )
        },
        store_backend_defaults=S3StoreBackendDefaults(default_bucket_name=bucket_name),
        stores={
            "expectations_S3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "great_expectations/expectations/" + actualresource,
                },
            },
            "validations_S3_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "great_expectations/validation_result/" + actualresource,
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_S3_store",
        validations_store_name="validations_S3_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "great_expectations/data_docs/" + actualresource
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            }
        },
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                ],
            }
        },
        anonymous_usage_statistics={
            "enabled": True
        }
    )
    run_id={"run_name": actualresource, "run_time": datetime.datetime.now(datetime.timezone.utc)}
    context = BaseDataContext(project_config=project_config)
    expectation_suite_name = "validation"
    suite = context.get_expectation_suite(expectation_suite_name)
    batch_kwargs = context.build_batch_kwargs(data_asset_name=actualresource,
                                            batch_kwargs_generator='pandas_s3_generator', datasource='pandas_s3')
    batch = context.get_batch(batch_kwargs, suite)
    results = context.run_validation_operator("action_list_operator",assets_to_validate=[batch],
    result_format='COMPLETE',run_id=run_id)
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    if not results["success"]:
        send_mail("validation","Failure",results)
        s3_resource.Object(bucket_name,'great_expectations/validation_fail/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())
    else:
        # send_mail("validation","Success",results)
        s3_resource.Object(bucket_name,'great_expectations/validation_success/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())

def standardization_mapped(ds, **kwargs):
    os.system("aws configure set aws_access_key_id "+aws_access_key_id)
    os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
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
    input_df['id'] = input_df.index
    csv_buffer = StringIO()
    input_df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name,'mapped/'+actualresource+'/'+actualresource+'.csv').put(Body=csv_buffer.getvalue())


def ingest_to_smilecdr(ds, **kwargs):
    os.system("aws configure set aws_access_key_id "+aws_access_key_id)
    os.system("aws configure set aws_secret_access_key "+aws_secret_access_key)
    input_key = "mapped/"+actualresource+"/"+actualresource+".csv"
    input_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=input_key)
    input_df = pd.read_csv(input_file['Body'])
    ingestion_success = []
    ingestion_failure = []
    ingested_ids = []
    def ingest(i):
        row = input_df.iloc[i]
        coverage = Coverage.construct()
        identifier = Identifier.construct()
        identifiers = list()
        codeableconcept = CodeableConcept.construct()
        codeableconcept.text = row['identifier_type_text']
        identifier.value = str(row['identifier_value'])
        identifier.type = codeableconcept
        identifiers.append(identifier.dict())
        if row['type_coding_code'] == 'MA':
            if row['identifier_value1']:
                identifier1 = Identifier.construct()
                codeableconcept1 = CodeableConcept.construct()
                codeableconcept1.text = row['identifier_type_text1']
                identifier1.value = str(row['identifier_value1'])
                identifier1.type = codeableconcept1
                identifiers.append(identifier1.dict())
                identifier4 = Identifier.construct()
                codeableconcept4 = CodeableConcept.construct()
                codeableconcept4.text = row['identifier_type_text4']
                identifier4.value = str(row['identifier_value4'])
                identifier4.type = codeableconcept4
            else:
                identifier4 = Identifier.construct()
                codeableconcept4 = CodeableConcept.construct()
                codeableconcept4.text = row['identifier_type_text4']
                identifier4.value = str(row['identifier_value4'])
                identifier4.type = codeableconcept4
                identifiers.append(identifier4.dict())
        else:
            identifier1 = Identifier.construct()
            codeableconcept1 = CodeableConcept.construct()
            codeableconcept1.text = row['identifier_type_text1']
            identifier1.value = str(row['identifier_value1'])
            identifier1.type = codeableconcept1
            identifiers.append(identifier1.dict())
        identifier2 = Identifier.construct()
        codeableconcept2 = CodeableConcept.construct()
        codeableconcept2.text = row['identifier_type_text2']
        identifier2.value = str(row['identifier_value2'])
        identifier2.type = codeableconcept2
        identifiers.append(identifier2.dict())
        identifier3 = Identifier.construct()
        identifier3.value = str(row['identifier_value3'])
        identifier3.use = row['identifier_use3']
        identifier3.system = row['identifier_system3']
        identifiers.append(identifier3.dict())
        if row['type_coding_code'] == 'MA':
            if row['identifier_value1']:
                identifiers.append(identifier4.dict())
        if row['status'] in {'Active','active','Approved'}:
            row['status'] = 'active'
        elif row['status'] in {'Cancelled','cancelled'}:
            row['status'] = 'cancelled'
        elif row['status'] in {'Draft','draft','Pending','pending'}:
            row['status'] = 'draft'
        elif row['status'] in {'entered-in-error','Entered-in-error','Declined','declined'}:
            row['status'] = 'entered-in-error'
        coverage.status = row['status']
        coverage.extension = list()
        extension = Extension.construct()
        extension.url = row['extension_url']
        extension.valueString = row['extension_valueString']
        coverage.extension.append(extension)
        extension1 = Extension.construct()
        extension1.url = row['extension_url1']
        date = row['extension_valueString1'].split('-')
        extension1.valueString = date[1] + '-' + date[2] + '-' + date[0]
        coverage.extension.append(extension1)
        extension2 = Extension.construct()
        extension2.url = row['extension_url2']
        extension2.valueString = row['extension_valueString2']
        coverage.extension.append(extension2)
        extension3 = Extension.construct()
        extension3.url = row['extension_url3']
        extension3.valueString = str(row['extension_valueString3'])
        coverage.extension.append(extension3)
        extension4 = Extension.construct()
        extension4.url = row['extension_url4']
        extension4.valueString = row['extension_valueString4']
        coverage.extension.append(extension4)
        extension5 = Extension.construct()
        extension5.url = row['extension_url5']
        extension5.valueString = str(row['extension_valueString5'])
        coverage.extension.append(extension5)
        extension6 = Extension.construct()
        extension6.url = row['extension_url6']
        extension6.valueString = row['extension_valueString6']
        coverage.extension.append(extension6)
        extension7 = Extension.construct()
        extension7.url = row['extension_url7']
        extension7.valueString = row['extension_valueString7']
        coverage.extension.append(extension7)
        type = CodeableConcept.construct()
        type.coding = list()
        coding = Coding.construct()
        coding.code = row['type_coding_code']
        coding.display = row['type_coding_display']
        type.coding.append(coding)
        coverage.type = type
        first_name = row['first_name']
        last_name = row['last_name']
        if row['email'] =='':
            telecom = row['phone']
        else:
            telecom = row['email']
        if pd.isna(row['pid']):
            try:
                patient_response = requests.get(target_ingestion_endpoint+'DEFAULT/Patient?given='+first_name+
                            '&family='+last_name+'&telecom='+telecom,auth=(target_ingestion_username ,
                             target_ingestion_password))
                patient_result = patient_response.json()
                patient_id = patient_result['entry'][0]['resource']['id']
            except:
                ingestion_failure.append("No Patient present in the database with name = {} {}".format(row['first_name'],row['last_name']))
                print("No Patient with the name {} {} present in the database".format(first_name,last_name))
                patient_response = requests.get(target_ingestion_endpoint+'DEFAULT/Patient?given=ehealth&family=outofnetwork patients'
                            ,auth=(target_ingestion_username,target_ingestion_password))
                patient_result = patient_response.json()
                patient_id = patient_result['entry'][0]['resource']['id']
        else:
            try:
                patient_response = requests.get(target_ingestion_endpoint+tenant_name+'/Patient/'+str(int(row['pid']))
                            ,auth=(target_ingestion_username,target_ingestion_password))
                patient_result = patient_response.json()
                patient_id = patient_result['id']
            except:
                ingestion_failure.append("No Patient present in the database with ID = {}".format(str(int(row['pid']))))
                print("No Patient with the ID = {} present in the database".format(str(int(row['pid']))))
                patient_response = requests.get(target_ingestion_endpoint+'DEFAULT/Patient?given=ehealth&family=outofnetwork patients'
                            ,auth=(target_ingestion_username,target_ingestion_password))
                patient_result = patient_response.json()
                patient_id = patient_result['entry'][0]['resource']['id']
        reference1 = Reference.construct()
        reference1.reference = 'Patient/'+str(int(patient_id))
        reference2 = Reference.construct()
        reference2.reference = 'Patient/'+str(int(patient_id))
        reference3 = Reference.construct()
        reference3.reference = 'Patient/'+str(int(patient_id))
        coverage.policyHolder = reference1
        coverage.subscriber = reference2
        coverage.beneficiary = reference3
        coverage_json = coverage.dict()
        coverage_json['identifier'] = identifiers
        coverage_json['period'] = {}
        if str(row['period_start']) != 'nan':
            coverage_json['period']['start'] = str(row['period_start'])
        if str(row['period_end']) != 'nan':
            coverage_json['period']['end'] = str(row['period_end'])
        coverage_json['class'] = []
        if row['class_type_text'] in {'MA','MD'}:
            class_value = str(row['cms_plan_id'])
        else:
            class_value = str(row['plan_id'])
        class_json = {}
        class_json['value'] = class_value
        class_json['name'] = row['class_name']
        class_json['type'] = {}
        class_json['type']['text'] = row['class_type_text']
        coverage_json['class'].append(class_json)
        json_object= json.dumps(coverage_json,indent=4)
        print(i)
        with open('Coverage.json',"w") as outfile:
            outfile.write(json_object)
        try:
            response = requests.post(target_ingestion_endpoint+tenant_name+'/'+actualresource,
                    json=coverage_json,
                    auth=(target_ingestion_username , target_ingestion_password)).json()
            id = response['id']
            ingestion_success.append("Ingested to smilecdr")
            ingested_ids.append(row['id'])
        except:
            print('Coverage Creation failed')
            ingestion_failure.append("Coverage Creation failed for {} {}".format(first_name,last_name))
    # for i in range(len(input_df)):
    #     ingest(i)
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        res = [executor.submit(ingest,i) for i in range(len(input_df))]
        concurrent.futures.wait(res)
        results = {}
        results['failure'] = ingestion_failure
        results['success'] = ingestion_success
        if len(results['failure'])>0:
            send_mail("failure","ingest_to_smilecdr",results)
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
    send_mail("success",None,None)
        
        
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

task_concatenate >>  task_standardization_mapped >> task_ingest_to_smilecdr
