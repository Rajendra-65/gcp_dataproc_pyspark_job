from datetime import datetime,timedelta
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import(
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

default_args = {
    'owner':'airflow',
    'depends_on_past':True,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes= 5)
}

dag = DAG(
    'gcp_dataproc_pyspark_dag',
    default_args=default_args,
    description = 'An dag to create the cluster and run job',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2024,9,2),
    catchup = False,
    tags = ['dev']
)

CLUSTER_NAME = 'spark-cluster-by-airflow'
PROJECT_ID = 'smiling_castle-431016-v3'
REGION = 'us-east1'
CLUSTER_CONFIG = {
    'master_config' : {
        'num_instances':1,
        'machine_type_uri':'n1-standard-2',
        'disk_config':{
            'boot_disk_type':'pd-standard',
            'boot_disk_size_gb':30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config':{
        'image_version':'2.2.26-debian12'
    }
}

create_cluster = DataprocCreateClusterOperator(
    task_id = 'create_cluster',
    cluster_name = CLUSTER_NAME,
    project_id = PROJECT_ID,
    region = REGION,
    cluster_config = CLUSTER_CONFIG,
    dag = dag
)

pyspark_job = {
    'main_python_file_uri':'gs://airflow_assignment_1/spark_code_container/orders_processing_batch_job.py'
}

current_date_formatted = datetime.now().strftime('%Y%m%d')

object_exist_sensor = GCSObjectExistenceSensor(
    task_id = 'Object_Existance_check',
    bucket = f'gs://airflow_assignment_1',
    object = f'gs://airflow_assignment_1/raw_data/orders_{current_date_formatted}.csv',
    google_cloud_conn_id='google_cloud_default',
    dag = dag
)

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id = 'submit_pyspark_job',
    main = pyspark_job['main_python_file_uri'],
    arguments=[current_date_formatted],
    cluster_name = CLUSTER_NAME,
    region = REGION,
    project_id = PROJECT_ID,
    dag = dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)

create_cluster >> object_exist_sensor >> submit_pyspark_job >> delete_cluster