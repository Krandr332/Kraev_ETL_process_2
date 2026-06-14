import datetime
import uuid

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_FOLDER_ID = 'b1gmnqhru3ra51eb9t4d'
YC_DP_SUBNET_ID = 'ajcniai8l1jogfasfo00'
YC_DP_SA_ID = 'ajesgnd83eknv9abkmdr'
YC_DP_AZ = 'ru-central1-e'
YC_S3_BUCKET = 'basket'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPHmVMj1UBndZgPuuC96A0JT9bdOZGWCNacdTV7KTiir mr.krandr@mail.ru'

INPUT_PATH = f"s3a://{YC_S3_BUCKET}/input"
OUTPUT_PATH = f"s3a://{YC_S3_BUCKET}/output"
SCRIPT_URI = f"s3a://{YC_S3_BUCKET}/scripts/process_applications.py"

default_args = {
    "owner": "kraev",
    "start_date": datetime.datetime(2026, 6, 13),
}

with DAG(
    dag_id="exam_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["exam", "dataproc"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"exam-cluster-{uuid.uuid4().hex[:8]}",
        cluster_description="Temporary cluster for exam ETL",
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_S3_BUCKET,  
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version="2.1",
        ssh_public_keys=[YC_DP_SSH_PUBLIC_KEY],
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-ssd",
        masternode_disk_size=32,
        datanode_count=0,
        services=["YARN", "SPARK"],
        connection_id="yandexcloud_default",
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_processing",
        main_python_file_uri=SCRIPT_URI,
        args=[
            "--input-path", INPUT_PATH,
            "--output-path", OUTPUT_PATH,
        ],
        properties={
            "spark.hadoop.fs.s3a.endpoint": "storage.yandexcloud.net",
        },
        connection_id="yandexcloud_default",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        connection_id="yandexcloud_default",
    )

    create_cluster >> run_pyspark >> delete_cluster