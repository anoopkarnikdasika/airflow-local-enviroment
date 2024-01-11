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
from fhir.resources.insuranceplan import InsurancePlan
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
actualresource = 'InsurancePlan'
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
        body = """<html> <body> <p>Hi, </p> <p>Ehealth InsurancePlan Ingestion completed.</p></body></html>"""
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
        insuranceplan = InsurancePlan.construct()
        insuranceplan.identifier = list()
        identifier = Identifier.construct()
        codeableconcept = CodeableConcept.construct()
        codeableconcept.text = row['identifier_type_text']
        if row['type_text'] in {'MA','MD'}:
            identifier.value = str(row['cms_plan_id'])
        else:
            identifier.value = str(row['plan_id'])
        identifier.type = codeableconcept
        insuranceplan.identifier.append(identifier)
        codeableconcept1 = CodeableConcept.construct()
        codeableconcept1.text = row['identifier_type_text1']
        identifier1 = Identifier.construct()
        identifier1.value = str(row['identifier_value1'])
        identifier1.type = codeableconcept1
        insuranceplan.identifier.append(identifier1)
        identifier2 = Identifier.construct()
        identifier2.use = row['identifier_use2']
        identifier2.system = row['identifier_system2']
        identifier2.value = str(row['identifier_value2'])
        insuranceplan.identifier.append(identifier2)
        insuranceplan.type = list()
        type = CodeableConcept.construct()
        type.text = row['type_text']
        insuranceplan.type.append(type)
        insuranceplan.name = row['name']
        insuranceplan.extension = list()
        extension = Extension.construct()
        extension.url = row['extension_url']
        extension.valueString = str(row['extension_valueString'])
        insuranceplan.extension.append(extension)
        insuranceplan_json = insuranceplan.dict()
        insuranceplan_json['period'] = {}
        insuranceplan_json['period']['start'] = str(row['period_start'])
        insuranceplan_json['plan'] = []
        plan = {}
        plan['identifier'] = []
        plan_identifier = {}
        plan_identifier['value'] = str(row['plan_identifier_value'])
        plan['identifier'].append(plan_identifier)
        insuranceplan_json['plan'].append(plan)
        # json_object= json.dumps(insuranceplan_json,indent=4)
        # with open('InsurancePlan.json',"w") as outfile:
        #     outfile.write(json_object)
        try:
            response = requests.post(target_ingestion_endpoint+tenant_name+'/'+actualresource,
                    json=insuranceplan_json,
                    auth=(target_ingestion_username , target_ingestion_password)).json()
            id = response['id']
            ingestion_success.append("Ingested to smilecdr")
            ingested_ids.append(row['id'])
        except:
            print('In Creation failed')
            ingestion_failure.append("Insurance Plan Creation failed for {} {}".format(first_name,last_name))
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


def update_coverage_enrollment():
    count = 50
    offset = 0
    total = 50
    while total==50:
        created_on = datetime.datetime.now()
        updated_at = (created_on+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        identifier = 'EHEALTH'
        status = 'active'
        print(target_ingestion_endpoint+tenant_name+'/Coverage?identifier='+identifier+'&_lastUpdated=gt'+updated_at+'&_count='+str(count)+'&_offset='+str(offset))
        try:
            coverage_response = requests.get(target_ingestion_endpoint+tenant_name+'/Coverage?identifier='+identifier+'&_lastUpdated=gt'+updated_at+'&_count='+str(count)+'&_offset='+str(offset),
                                auth=(target_ingestion_username, target_ingestion_password))
            coverage_result = coverage_response.json()
            coverages = coverage_result['entry']
            total = len(coverages)
            offset+=total
        except:
            print("No Coverages Found")
        for coverage in coverages:
            coverage_resource = coverage['resource']
            for identifier in coverage_resource['identifier']:
                if 'type' in identifier:
                    if identifier['type']['text'] == "transaction_id":
                        transaction_id = identifier['value']
                    elif identifier['type']['text'] == 'policy_number':
                        policy_number = identifier['value']
                    elif identifier['type']['text'] == 'cms_contract_id/carrier_id':
                        cms_contract_id = identifier['value']
            status = coverage_resource['status']
            print(transaction_id)
            if transaction_id!='nan':
                policy_type = coverage_resource['type']['coding'][0]['code']
                extensions = coverage_resource['extension']
                for extension in extensions:
                    if extension['url'] == 'county':
                        county = extension['valueString']
                    elif extension['url'] == 'birth_date':
                        birth_date = extension['valueString']
                    elif extension['url'] == 'gender':
                        gender = extension['valueString']
                    elif extension['url'] == 'zip':
                        zip = extension['valueString']
                    elif extension['url'] == 'Csr Tier':
                        csr_tier = extension['valueString']
                    elif extension['url'] == 'Alliance ID':
                        alliance_id = extension['valueString']
                plan_id = coverage_resource['class'][0]['value']
                print(target_ingestion_endpoint+tenant_name+'/InsurancePlan?identifier='+plan_id+'&identifier='+cms_contract_id)
                try:
                    insuranceplan_response = requests.get(target_ingestion_endpoint+tenant_name+'/InsurancePlan?identifier='+plan_id+'&identifier='+cms_contract_id,
                                        auth=(target_ingestion_username , target_ingestion_password))
                    insuranceplan_result = insuranceplan_response.json()
                    insuranceplans = insuranceplan_result['entry']
                except:
                    print("No InsurancePlan Found with plan_id = {} & cms_contract_id = {}".format(plan_id,cms_contract_id))
                insuranceplan_resource = insuranceplans[0]['resource']
                cms_segment_id = insuranceplan_resource['extension'][0]['valueString']
                start_date = coverage_resource['period']['start']
                planYear = start_date[:4]
                expiry_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=364)).strftime("%Y-%m-%d")
                if policy_type == 'MA':
                    while len(plan_id)<3:
                        plan_id = '0'+plan_id
                    while len(zip)<5:
                        zip = '0'+zip
                    while len(cms_segment_id)<3:
                        cms_segment_id+='0'
                    hotlink = """https://ehealthmedicareplans.com/ehi/medicare/fast-quote?allid={}&zip={}
                    &county={}&planYear={}&plans={}:{}:{}""".format(alliance_id,zip,county,planYear,cms_contract_id,plan_id,cms_segment_id)
                elif policy_type == 'IFP' and csr_tier=="0":
                    hotlink = """https://www.ehealthinsurance.com/?type=IFP&allid={}&showPlanDetails=true&
                    zip={}&county={}&gd1={}&bdate1={}&carrierid={}&planid={}""".format(alliance_id,zip,county,
                    gender,birth_date,cms_contract_id,plan_id)
                else:
                    hotlink = ""
                insurer = "ehealth"
                search_query = """Update coverage_schema.coverage_enrollment set 
                status = '{}',
                start_date = '{}',
                expiry_date = '{}',
                policy_number = '{}',
                hot_link = '{}',
                insurer = '{}'
                where sid={} and status='Pending'""".format(status,start_date,expiry_date,policy_number,hotlink,insurer,transaction_id,"Pending")
                # print(search_query)
                conn = psycopg2.connect(
                    "host='"+mpowered_host+"' port='"+mpowered_port+"' dbname='"+mpowered_database+"' user='"+mpowered_user+"'"
                    "password='"+mpowered_password+"'")
                cursor = conn.cursor()
                cursor.execute(search_query)
                # result = cursor.fetchall();
                # print(result)
                cursor.close()
                conn.commit()
                conn.close()
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
task_update_coverage_enrollment = PythonOperator(
    task_id='task_update_coverage_enrollment',
    python_callable=update_coverage_enrollment,
    provide_context=True,
    dag=dag)

task_concatenate >> task_standardization_mapped >> task_ingest_to_smilecdr >> task_update_coverage_enrollment