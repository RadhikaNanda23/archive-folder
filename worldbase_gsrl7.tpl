from airflow import DAG
from worldbase_airflow.dag_definitions.worldbase_refinein_dag import WBSRefineinDAG

dag_name= '{{ dag_name }}'

wbs_dag_params = {
    'prscflow': 'REFINEINPROCESS',
    'process_log_name': 'worldbase-airflow-process-log',
    'source_key': '{{ state_code }}',      ###################need to get it from the template to dag conversion
    'data_source_name':  '{{ gts_data_source_name}}',    ###################need to get it from the template to dag conversion
    'domain_name': '{{ pipeline_name }}',    ###############need to get it from the template to dag conversion
    'source_category': 'gsrl',
    'common_source_category': 'gsrl',
    'template_name': 'worldbase_gsrl7.tpl'
}


wbs_dag = WBSRefineinDAG(dag_name, wbs_dag_params)

globals()[dag_name] = wbs_dag.return_dag_object()