FROM apache/airflow:2.3.0
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
USER root
RUN apt-get update 
RUN echo Y | apt-get install awscli
RUN aws configure set aws_access_key_id {{aws_access_key_id}} --profile default
RUN aws configure set aws_secret_access_key {{aws_secret_access_key}} --profile default