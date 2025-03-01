from airflow import DAG
from worldbase_airflow.dag_definitions.worldbase_refinein_dag import WBSRefineinDAG

dag_name= 'worldbase_gsrl7_worldbase_gsrl7unitedstates'

wbs_dag_params = {
    'prscflow': 'REFINEINPROCESS',
    'process_log_name': 'worldbase-airflow-process-log',
    'source_key': 'gsrl7unitedstates',      ###################need to get it from the template to dag conversion
    'data_source_name':  'GSRL7 UNITEDSTATES',    ###################need to get it from the template to dag conversion
    'domain_name': 'worldbase',    ###############need to get it from the template to dag conversion
    'source_category': 'gsrl',
    'common_source_category': 'gsrl',
    'template_name': 'worldbase_gsrl7.tpl'
}


wbs_dag = WBSRefineinDAG(dag_name, wbs_dag_params)

globals()[dag_name] = wbs_dag.return_dag_object()