import base64
import json
import os
from datetime import datetime, timedelta
from time import time
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcSparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

dag_name = 'cohart_analysis_scheduling'.strip()


def push_cluster_name(**kwargs):
  ti = kwargs['ti']
  cluster_name = dag_name[:27] + '-' + str(int(round(time() * 100)))
  ti.xcom_push(key='cluster_name', value=cluster_name)


with DAG(
    dag_id=dag_name,
    schedule_interval='@daily',
    start_date=datetime.strptime('2019-08-29 00:00:00', "%Y-%m-%d %H:%M:%S"),
    max_active_runs=1,
    concurrency=10,
    default_args={
        'project_id': 'makoto0908spark',
        'email': 'test@gmail.com',
        'email_on_failure': True,
        'email_on_retry': False
    }) as dag:

  push_cluster_name = PythonOperator(
      dag=dag,
      task_id="push-cluster-name",
      provide_context=True,
      python_callable=push_cluster_name)

  dataproc_create_cluster_1 = DataprocClusterCreateOperator(
      task_id='dataproc_create_cluster_1',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '1',
      region='us-west1',
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2',
      num_workers=2,
      execution_timeout=timedelta(minutes=30))

  dataproc_create_cluster_1.set_upstream(push_cluster_name)

  dataproc_destroy_cluster_1 = DataprocClusterDeleteOperator(
      task_id='dataproc_destroy_cluster_1',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '1',
      region='us-west1',
      execution_timeout=timedelta(minutes=30),
      trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

  dataproc_create_cluster_2 = DataprocClusterCreateOperator(
      task_id='dataproc_create_cluster_2',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '2',
      region='us-west1',
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2',
      num_workers=2,
      execution_timeout=timedelta(minutes=30))

  dataproc_create_cluster_2.set_upstream(push_cluster_name)

  dataproc_destroy_cluster_2 = DataprocClusterDeleteOperator(
      task_id='dataproc_destroy_cluster_2',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '2',
      region='us-west1',
      execution_timeout=timedelta(minutes=30),
      trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

  dataproc_create_cluster_3 = DataprocClusterCreateOperator(
      task_id='dataproc_create_cluster_3',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '3',
      region='us-west1',
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2',
      num_workers=2,
      execution_timeout=timedelta(minutes=30))

  dataproc_create_cluster_3.set_upstream(push_cluster_name)

  dataproc_destroy_cluster_3 = DataprocClusterDeleteOperator(
      task_id='dataproc_destroy_cluster_3',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '3',
      region='us-west1',
      execution_timeout=timedelta(minutes=30),
      trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

  dataproc_create_cluster_4 = DataprocClusterCreateOperator(
      task_id='dataproc_create_cluster_4',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '4',
      region='us-west1',
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2',
      num_workers=2,
      execution_timeout=timedelta(minutes=30))

  dataproc_create_cluster_4.set_upstream(push_cluster_name)

  dataproc_destroy_cluster_4 = DataprocClusterDeleteOperator(
      task_id='dataproc_destroy_cluster_4',
      project_id='makoto0908spark',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '4',
      region='us-west1',
      execution_timeout=timedelta(minutes=30),
      trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

  args = ["--process.date", "{{ (execution_date).strftime('%Y-%m-%d') }}"]

  unique_user = DataProcSparkOperator(
      task_id='unique_user',
      dataproc_spark_jars=[
          'gs://path/jar/CohortAnalysis.jar'
      ],
      main_class='com.makoto.spark.process.UserGenerateProcess',
      region='us-west1',
      job_name=dag_name + 'unique_user',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '1',
      execution_timeout=timedelta(minutes=180),
      arguments=args)

  args = ["--process.date", "{{ (execution_date).strftime('%Y-%m-%d') }}"]

  bike_share_aggregator = DataProcSparkOperator(
      task_id='bike_share_aggregator',
      dataproc_spark_jars=[
          'gs://path/jar/CohortAnalysis.jar'
      ],
      main_class='com.makoto.spark.process.BikeTripProcess',
      region='us-west1',
      job_name=dag_name + 'bike_share_aggregator',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '1',
      execution_timeout=timedelta(minutes=120),
      arguments=args)

  args = [
      "--process.date", "{{ (execution_date).strftime('%Y-%m-%d') }}",
      "--day.ago", "1"
  ]

  bike_share_retention_d1 = DataProcSparkOperator(
      task_id='bike_share_retention_d1',
      dataproc_spark_jars=[
          'gs://path/jar/CohortAnalysis.jar'
      ],
      main_class='com.makoto.spark.process.RetentionComputeProcess',
      region='us-west1',
      job_name=dag_name + 'bike_share_retention_d1',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '2',
      execution_timeout=timedelta(minutes=180),
      arguments=args)

  args = [
      "--process.date", "{{ (execution_date).strftime('%Y-%m-%d') }}",
      "--day.ago", "3"
  ]

  bike_share_retention_d3 = DataProcSparkOperator(
      task_id='bike_share_retention_d3',
      dataproc_spark_jars=[
          'gs://path/jar/CohortAnalysis.jar'
      ],
      main_class='com.makoto.spark.process.RetentionComputeProcess',
      region='us-west1',
      job_name=dag_name + 'bike_share_retention_d3',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '3',
      execution_timeout=timedelta(minutes=180),
      arguments=args)

  args = [
      "--process.date", "{{ (execution_date).strftime('%Y-%m-%d') }}",
      "--day.ago", "7"
  ]

  bike_share_retention_d7 = DataProcSparkOperator(
      task_id='bike_share_retention_d7',
      dataproc_spark_jars=[
          'gs://path/jar/CohortAnalysis.jar'
      ],
      main_class='com.makoto.spark.process.RetentionComputeProcess',
      region='us-west1',
      job_name=dag_name + 'bike_share_retention_d7',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}'
      + '4',
      execution_timeout=timedelta(minutes=180),
      arguments=args)

    # check for every 1 min = poke_interval if _SUCCESS, the status of computed unique user list json exist 
    # timeout be 1 hour
    # if completed user list generation, trigger down stream jobs
  unique_user_sensor = GoogleCloudStorageObjectSensor(
      task_id='unique_user_sensor',
      bucket='bucketname',
      object='bike/unique-user/_SUCCESS',
      poke_interval=30,
      timeout=6000)

  unique_user.set_upstream(dataproc_create_cluster_1)

  unique_user.set_downstream(bike_share_aggregator)

  bike_share_aggregator.set_downstream(dataproc_destroy_cluster_1)

  bike_share_retention_d1.set_upstream(dataproc_create_cluster_2)

  bike_share_retention_d1.set_downstream(dataproc_destroy_cluster_2)

  bike_share_retention_d3.set_upstream(dataproc_create_cluster_3)

  bike_share_retention_d3.set_downstream(dataproc_destroy_cluster_3)

  bike_share_retention_d7.set_upstream(dataproc_create_cluster_4)

  bike_share_retention_d7.set_downstream(dataproc_destroy_cluster_4)

  dataproc_create_cluster_1.set_upstream(unique_user_sensor)

  dataproc_create_cluster_2.set_upstream(dataproc_destroy_cluster_1)

  dataproc_create_cluster_3.set_upstream(dataproc_destroy_cluster_1)

  dataproc_create_cluster_4.set_upstream(dataproc_destroy_cluster_1)
