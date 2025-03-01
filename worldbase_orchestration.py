import os
from airflow import DAG
import json
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dnb_airflow_rf.utils.gcp_logger import Logger
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from dnb_airflow_rf.utils.env_config import EnvConfig
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from dnb_airflow_rf.tools.api_wrapper import APIWrapper
from uuid import uuid4


def handle_messages(pulled_messages, context):
    #dag_ids = list()
    task_instance = context["ti"]
    if len(pulled_messages) == 0:
        raise AirflowSkipException
    for idx, m in enumerate(pulled_messages):
        data = m.message.data.decode("utf-8")
        print(f"message {idx} data is {data}")
        json_data = json.loads(data)
        source_key = json_data['data_source']['key']
        trigger_dag_id = f"worldbase_gsrl7_worldbase_{source_key}"
        source_id = json_data["data_source"]["id"]
        end_to_end_key =  json_data["end_to_end_key"]
        consumer_id = json_data["consumer"]["id"]
        consumer_name = json_data["consumer"]["name"]
        parent_batch_id = json_data["discovery_batch_id"]
        batch_id = json_data["batch_id"]
        load_type = json_data["load_type"]
        task_instance.xcom_push(key="trigger_dag_id", value=trigger_dag_id)
        task_instance.xcom_push(key="source_key", value=source_key)
        task_instance.xcom_push(key="source_id", value=source_id)
        task_instance.xcom_push(key="end_to_end_key", value=end_to_end_key)
        task_instance.xcom_push(key="consumer_id", value=consumer_id)
        task_instance.xcom_push(key="consumer_name", value=consumer_name)
        task_instance.xcom_push(key="load_type", value=load_type)
        task_instance.xcom_push(key="dag_run_conf", value=json_data)
        task_instance.xcom_push(key="process_log_name", value="worldbase-airflow-process-log")
        task_instance.xcom_push(key="parent_batch_id", value=parent_batch_id)
        task_instance.xcom_push(key="batch_id", value=batch_id)
    return

def create_master_audit(**kwargs):
    task_instance = kwargs["ti"]
    api_wrapper = APIWrapper()
    process_id = str(uuid4())
    master_audit_kwargs = {
        "prscflow": "REFINEINPROCESS",
        "source_id": task_instance.xcom_pull('pull_messages_operator', key='source_id'),
        "process_id": process_id,
        "end_to_end_key": task_instance.xcom_pull('pull_messages_operator', key='end_to_end_key'),
        "consumer_id": task_instance.xcom_pull('pull_messages_operator', key='consumer_id'),
        "consumer_name":task_instance.xcom_pull('pull_messages_operator', key='consumer_name'),
        "pipeline": "WORLDBASE_GSRL7",
        "batchtype":task_instance.xcom_pull('pull_messages_operator', key='load_type'),
        "dag_id": task_instance.xcom_pull('pull_messages_operator', key='trigger_dag_id'),
        "process_log_name": task_instance.xcom_pull('pull_messages_operator', key='process_log_name'),
        "source_key": task_instance.xcom_pull('pull_messages_operator', key='source_key'),
        "parent_batch_id": task_instance.xcom_pull('pull_messages_operator', key='parent_batch_id'),
        "batch_id": task_instance.xcom_pull('pull_messages_operator', key='batch_id')
    }
    reply =api_wrapper.create_master_audit(**master_audit_kwargs, **kwargs)
    master_audit_response = reply['response']
    Logger.debug(f"{master_audit_response}")
    task_instance.xcom_push(key='master_audit_response', value=master_audit_response)
    return

def create_dag_run_conf(**kwargs):
    Logger.debug(f"Creating DAG Run conf")
    task_instance = kwargs['ti']
    master_audit_response = task_instance.xcom_pull('create_master_audit_id', key='master_audit_response')
    trigger_dag_run_conf = {}
    trigger_dag_run_conf["batchid"] = master_audit_response['batchid']
    trigger_dag_run_conf['prcsid'] = master_audit_response['prcsid']
    trigger_dag_run_conf['prntbatchid'] = master_audit_response['prntbatchid']
    trigger_dag_run_conf['srckey'] = task_instance.xcom_pull('pull_messages_operator', key='source_key')
    trigger_dag_run_conf['srcid'] = master_audit_response['srcid']
    trigger_dag_run_conf['cnsid'] = master_audit_response['cnsid']
    trigger_dag_run_conf['cnsname'] = master_audit_response['cnsname']
    trigger_dag_run_conf['endtoendkey'] = master_audit_response['endtoendkey']
    trigger_dag_run_conf['master_audit_response'] = master_audit_response
    trigger_dag_run_conf['master_audit_response']['custmtd']['refine_notification_info'] = task_instance.xcom_pull('pull_messages_operator', key='dag_run_conf')
    
    Logger.debug(f"DAG Run conf: {trigger_dag_run_conf}")
    task_instance.xcom_push(key='dag_run_conf', value=trigger_dag_run_conf)
    return

def fetch_config(**kwargs):
    task_instance = kwargs["ti"]
    env_config = EnvConfig()
    project_id = env_config.get("project_id")
    topic_id = env_config.get("worldbase_topic_id")
    subscription_id =  env_config.get("worldbase_subscription_id")
    task_instance.xcom_push(key="project_id", value=project_id)
    task_instance.xcom_push(key="topic_id", value=topic_id)
    task_instance.xcom_push(key="subscription_id", value=subscription_id)
    return

dag = DAG(
    dag_id="worldbase_orchestration_dag",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
    tags=["worldbase", "mdd", "refineinprocess"]
)

fetch_env_config = PythonOperator(
    task_id="fetch_env_config",
    python_callable=fetch_config,
    provide_context=True,
    dag=dag
)

pull_messages_operator = PubSubPullOperator(
    task_id="pull_messages_operator",
    project_id="{{task_instance.xcom_pull('fetch_env_config', key='project_id')}}",  
    ack_messages=True,
    messages_callback=handle_messages,
    subscription="{{task_instance.xcom_pull('fetch_env_config', key='subscription_id')}}",
    max_messages=1,
    dag=dag
)

create_master_audit_id = PythonOperator(
    task_id="create_master_audit_id",
    python_callable=create_master_audit,
    provide_context=True,
    dag=dag
)

create_dag_run_config = PythonOperator(
    task_id = "create_dag_run_conf",
    python_callable=create_dag_run_conf,
    provide_context = True,
    dag=dag
)

trigger_worldbase_gsrl7_dag = TriggerDagRunOperator(
    task_id="trigger_worldbase_gsrl7_dags",
    trigger_dag_id="{{task_instance.xcom_pull('pull_messages_operator', key='trigger_dag_id')}}",
    conf="{{ task_instance.xcom_pull('create_dag_run_conf', key='dag_run_conf') }}",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)


fetch_env_config >> pull_messages_operator >> create_master_audit_id >>  create_dag_run_config >> trigger_worldbase_gsrl7_dag