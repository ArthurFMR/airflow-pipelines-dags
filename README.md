# Demo of Apache Airflow for Macdonald Nutrients and International Airlines ETLs Pipelines
https://airflow-pipeline-dags.herokuapp.com/
## Login to Apache Airflow
username: usertest
password: usertest

# Try it Locally
for more specific directions go to the official website https://airflow.apache.org/docs/apache-airflow/stable/start/local.html

1. After clone the repository, install requirements `pip install -r requirements.txt`
2. SET the next environment variables:
..* `AIRFLOW__CORE__LOAD_EXAMPLES=False` this is for turn of the examples that has Airflow for default 
3. Execute the next command to initialize the database `airflow db init`
4. create a user for the UI 
```
airflow users create \
    --username admin \
    --firstname user \
    --lastname test \
    --role Admin \
    --email admin@gmail.org
```
5. Execute the webserver `airflow webserver` and execute the scheduler `airflow scheduler`
