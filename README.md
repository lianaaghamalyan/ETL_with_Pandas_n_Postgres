# ETL_with_Pandas_n_Postgres
This project involves fetching a remote CSV file, transferring it to a local PostgreSQL table, and utilizing pandas to refine and organize the data. Airflow is employed to orchestrate this process as a cohesive workflow.


## STEPS
* write_csv_to_postgres.py -> gets a csv file from a URL. Saves it into the local working directory as churn_modelling.csv. After reading the csv file, it writes it to a local PostgreSQL table

* create_df_and_modify.py -> reads the same Postgres table and creates a pandas dataframe out of it, modifies it. Then, creates 3 separate dataframes.

* write_df_to_postgres.py -> writes these 3 dataframes to 3 separate tables located in Postgres server.

<br>

Install necessary libraries and packages:
> pip install -r requirements.txt

<br>

Install PostgreSQL and configure it with the necessary credentials. You can use tools like pgAdmin or DBeaver to manage your PostgreSQL environment.


### Setting Up Airflow DAG
Let's automate the process a bit using the Airflow DAGs. We will be running Airflow as a Docker container. I used Puckel’s repo to run.

* Run the following command to clone the necessary repo on your local:
> git clone https://github.com/puckel/docker-airflow

* Run the following command so that all dependencies are configured:
> docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

* Aaand use the docker-compose.yaml file to run the Airflow as a Docker container.
> docker-compose -f docker-compose-LocalExecutor.yml up -d

<br>

Now you have a running Airflow container and you can access the UI at https://localhost:8080   :)
