from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Kafka producer function
def kafka_producer():
    import subprocess
    subprocess.run(['python', '/path/to/kafka_producer.py'])

with DAG('streaming_pipeline', start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:
    produce_data = PythonOperator(
        task_id='kafka_producer',
        python_callable=kafka_producer
    )

    spark_streaming = KubernetesPodOperator(
        task_id='spark_streaming_job',
        name='spark_streaming',
        namespace='default',
        image='bitnami/spark:latest',
        cmds=['spark-submit', '/path/to/spark_streaming_script.py'],
        is_delete_operator_pod=True
    )

    produce_data >> spark_streaming

