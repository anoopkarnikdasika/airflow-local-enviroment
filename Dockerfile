FROM apache/airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
USER root
RUN apt-get update 
RUN echo Y | apt-get install awscli
RUN aws configure set aws_access_key_id AKIASO23EYD2V7CN4VHJ --profile default
RUN aws configure set aws_secret_access_key 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 --profile default