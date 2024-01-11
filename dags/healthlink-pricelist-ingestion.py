import pandas as pd
import gzip
import os
import ijson
import pandas as pd
import json
import time
import boto3
from datetime import date
import airflow
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import Variable
from elasticsearch import Elasticsearch,RequestsHttpConnection,helpers
from elasticsearch.helpers import bulk

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

entity_name = "Healthlink Inc."
entity_type = "Health insurance Issuer"
last_updated_on = "2022-07-01"
plan_name = "health link"
plan_id_type = "in-network-rate"
plan_id = "3"
affiliate_id = "7"
host = 'search-pricelist-bvgvrzu4rbe4iuguxgj2ei5z5e.us-east-1.es.amazonaws.com' #without 'https'
billing_code_key = "raw/utils/billing_code.csv"
input_folder = "raw/healthlink_pricelist/"
download_path =  "/home/airflow/"

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

default_args = {
    "owner": "Airflow"
    }

start_date = airflow.utils.dates.days_ago(1)

# The DAG definition
dag = DAG(
    dag_id="healthlink-pricelist-ingestion",
    schedule_interval = None,
    catchup = False,
    start_date = start_date,
    default_args=default_args
)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def data_extract(ds, **kwargs):
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('json.gz'):
            s3_client.download_file(bucket_name,objects.key,download_path+objects.key.split('/')[2])


def get_provider_references(ds, **kwargs):
    start = 1
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('json.gz'):
            input_key = download_path+objects.key.split('/')[2]
    input_data = gzip.open(input_key)
    time_start = time.time()
    os.system("mkdir "+download_path+"provider_references")
    for provider_references_objects in ijson.items(input_data,'provider_references.item'):
        if (start>0):
            with open(download_path+"provider_references/"+str(provider_references_objects['provider_group_id'])+".json", "w") as outfile:
                json.dump(provider_references_objects, outfile)
        start+=1 
        if start%10000==0:        
            print("time to create {} provider reference files in  {:.2f} mins".format(str(start),(time.time()-time_start)/60))

def send_network(ds, **kwargs):
    billing_code_file = s3_client.read_file = s3_client.get_object(Bucket=bucket_name,Key=billing_code_key)
    billing_code_list = pd.read_csv(billing_code_file['Body'])['Code'].tolist()
    start = 1
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('json.gz'):
            input_key = download_path+objects.key.split('/')[2]
    input_data = gzip.open(input_key)
    time_start = time.time()
    in_network_start = 1
    os.system("mkdir "+download_path+"network")
    for in_network_references_objects in ijson.items(input_data,'in_network.item'):
        if in_network_start>0 and in_network_references_objects['billing_code'] in billing_code_list:
            for negotiated_rate in in_network_references_objects['negotiated_rates']:
                provider_references = []
                for provider_reference in negotiated_rate['provider_references']:
                    provider_references.extend(json.load(open(download_path+'provider_references/'+str(provider_reference)+'.json'))['provider_groups'])
                for i in range(len(negotiated_rate['negotiated_prices'])):
                    negotiated_rate['negotiated_prices'][i]['negotiated_rate'] = float(negotiated_rate['negotiated_prices'][i]['negotiated_rate'])
                    if 'modifier' not in negotiated_rate['negotiated_prices'][i]:
                        negotiated_rate['negotiated_prices'][i]['modifier'] = 'na'
                payload = {
                    "plan_id":plan_id,
                    "negotiated_arrangement" :  in_network_references_objects['negotiation_arrangement'],
                    "coding_system_version": in_network_references_objects['billing_code_type_version'],
                    "plan_name":plan_name,
                    "entity_name":entity_name,
                    "entity_type":entity_type,
                    "affiliate_id":affiliate_id,
                    "billing_code":in_network_references_objects['billing_code'],
                    "plan_id_type":plan_id_type,
                    "service_name":in_network_references_objects['name'],
                    "coding_system":in_network_references_objects["billing_code_type"],
                    "last_updated_on":last_updated_on,
                    "provider_groups":provider_references,
                    "negotiated_prices":negotiated_rate['negotiated_prices']
                }
                if start>0:
                    with open(download_path+"network/network"+str(start)+".json", "w") as outfile:
                        json.dump(payload, outfile,cls=DecimalEncoder)
                start+=1
                if start%1000==0:
                    print("processed {} files".format(str(start)))
                    print("time to get network {:.2f} mins".format((time.time()-time_start)/60))
        else:
            start+=len(in_network_references_objects['negotiated_rates'])
        in_network_start += 1
        if in_network_start%1000==0:
            print("processed {} billing_codes and {} files around {} percentage completed".format(str(in_network_start),str(start),str(start*100.0000/3810000)))
            print("time to get billing_codes {:.2f} mins".format((time.time()-time_start)/60))
    print("processed {} billing_codes and {} files around {} percentage completed".format(str(in_network_start),str(start),str(start*100.0000/3810000)))
    print("time to get billing_codes {:.2f} mins".format((time.time()-time_start)/60))

def ingest_to_elastic_search(ds, **kwargs):
    bucket  = s3_resource.Bucket(bucket_name)
    for objects in bucket.objects.filter(Prefix=input_folder):
        if objects.key.endswith('json.gz'):
            price_list_file_name = objects.key.split('/')[2]
    index_name = 'price_list_plan_in_3_'+str(date.today())
    def generator(index_name):
        for file_name in os.listdir(download_path+"network"):
            yield {
                '_index':index_name,
                '_type':'_doc',
                '_source': json.load(open(download_path+"network/"+file_name))
            }

    def get_output_and_store():
        start_time = time.time()
        for success,info in helpers.parallel_bulk(client=es,actions=generator(index_name),chunk_size=20,thread_count=2,queue_size=8):
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
        "index.mapping.nested_objects.limit":1000000,
        "number_of_replicas" :0,
        "refresh_interval":"-1"
        },
        "mappings": {
            "dynamic": "false",
            "properties": {
                "billing_code": {
                    "type": "keyword"
                },
                "negotiated_prices": {
                    "type": "nested",
                    "properties": {
                        "billing_class": {
                                "type": "keyword"
                            },
                        "negotiated_rate": {
                            "type": "float"
                        },
                        "service_code": {
                            "type": "keyword"
                        },
                        "negotiated_type": {
                            "type": "keyword"
                        },
                        "modifier":{"type":"keyword"}
                    }
                },
                "plan_id": {
                    "type": "keyword"
                },
                "plan_id_type": {
                    "type": "keyword"
                },
                "plan_name": {
                    "type": "keyword"
                },
                "provider_groups": {
                    "type": "nested",
                    "properties": {
                        "npi": {
                            "type": "keyword"
                        }
                    }
                },
                "service_name": {
                    "type": "text",
                    "fields":{
                        "keyword":{
                            "type":"keyword"
                        }
                    }
                }
            }
        }
    }

    my =es.indices.create(index=index_name,body=Settings)
    res = es.indices.get_alias("*")
    try:
        get_output_and_store()
    except:
        a="error"
    # es.indices.update_aliases({
    #     "actions":[
    #         {"add":{"index":index_name,"alias":"price_list_plan_in_3"}},
    #         {"add":{"index":index_name,"alias":"price_list_plan_in_healthlink"}}
    #     ]
    # })
    os.system("rm -r "+download_path+"network")
    os.system("rm -r "+download_path+"provider_references")
    os.system("rm -r "+download_path+price_list_file_name)

task_data_extract = PythonOperator(
    task_id='task_data_extract',
    python_callable=data_extract,
    provide_context=True,
    dag=dag)

task_get_provider_references = PythonOperator(
    task_id='task_get_provider_references',
    python_callable=get_provider_references,
    provide_context=True,
    dag=dag)

task_send_network = PythonOperator(
    task_id='task_send_network',
    python_callable=send_network,
    provide_context=True,
    dag=dag)

task_ingest_to_elastic_search = PythonOperator(
    task_id='task_ingest_to_elastic_search',
    python_callable=ingest_to_elastic_search,
    provide_context=True,
    dag=dag)

task_data_extract >> task_get_provider_references >> task_send_network >> task_ingest_to_elastic_search