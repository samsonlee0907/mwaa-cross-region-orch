from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def create_emr(**kwargs):
    client = boto3.client("emr", region_name='ap-east-1')      

    cluster_id = client.run_job_flow(
        Name='mwaa_emr',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        ReleaseLabel='emr-6.0.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'MASTER',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'CORE',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ]
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ]
    )

    return cluster_id
    
def done():
    print('done')

args = {'owner': 'Anoymous', 'start_date': days_ago(2)}

with DAG(dag_id='hkemr_dag', default_args=args,
         schedule_interval='50 12 * * *', tags=['aws']) as dag:
    create_cluster = PythonOperator(task_id='create_cluster', python_callable=create_emr, dag=dag)
    done = PythonOperator(task_id='done', python_callable=done, dag=dag)
    create_cluster >> done
