# Initialization to new ec2 instance 

1) git clone this repo.
2) Build a docker image using the Dockerfile using the command - `docker build -t airflow-mpowered .`.
3) Use `docker-compose up -d` to start the container.
4) Airflow webui can be accessed through the ec2 container ip address and 8080 port.
5) In Admin Tab, Variables Section upload the environment specific json file provided seperately which will add the variables in the json file to the airflow environment for dags to use.
   
# Adding python libraries to airflow

1) Add the libraries to the requirements.txt file.
2) Build another docker image using the Dockerfile using the command - `docker build -t airflow-mpowered .`.
3) Type `docker-compose down ` to stop and remove the existing containers.
4) Type `docker-compose up -d` to start the container with the new image.

# Airflow local environment

This repository contains a docker-compose to get up and running a local Airflow environment for developing purposes.
Don't use this in production environments.

Start the environment with the `docker-compose up -d` command.
Remove the environment with the `docker-compose down` command.

Be aware that in the Dockerfile, it is not specified the Airflow Docker image, so the latest version will be installed.
It is recommended to specify the same version you have in your production environment.

# Get started

## Creating dags

Add your dags in the dags folder.

## Adding variables

In file secrets/variables.yaml you can add variables to be accessed in your DAGs.

## Adding connections

In file secrets/connections.yaml you can add connections to be accessed in your DAGs.

## User and password

When the environment is ready, a user and password authentication will be prompted.
By default, use these credentials:

- user: airflow
- pass: airflow
