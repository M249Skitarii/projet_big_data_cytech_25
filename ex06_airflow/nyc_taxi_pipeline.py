"""
DAG Airflow pour automatiser le pipeline complet NYC Taxi (VERSION SIMPLIFIÉE)
Pipeline séquentielle optimisée :
- Ex01 : Téléchargement des données vers MinIO (en parallèle avec Ex03)
- Ex03 : Création des tables SQL (en parallèle avec Ex01)
- Ex02 : Ingestion Spark vers PostgreSQL (après Ex01 ET Ex03)
- Ex05 : Entraînement du modèle ML (après Ex02) - OPTIONNEL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'bigdata',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 6),
}

dag = DAG(
    'nyc_taxi_pipeline',
    default_args=default_args,
    description='Pipeline pour le traitement des données NYC Taxi',
    schedule_interval='@daily',
    catchup=False,
    tags=['nyc-taxi', 'bigdata', 'ml'],
    params={
        'skip_ml': False,  # True pour sauter l'entraînement ML (Ex05)
    },
)


# ============================================================================
# FONCTIONS DE VÉRIFICATION
# ============================================================================

def should_run_ml(**context):
    """Décide si on exécute l'entraînement ML ou non"""
    skip_ml = context['params'].get('skip_ml', False)

    if skip_ml:
        print("skip_ml=True : entraînement ML sauté")
        return 'skip_ml_training'
    else:
        print("Lancement de l'entraînement ML")
        return 'ex05_copy_ml_code'


# ============================================================================
# TASKS - DÉMARRAGE
# ============================================================================

start = DummyOperator(task_id='start', dag=dag)


# ============================================================================
# TASKS - EX01 : TÉLÉCHARGEMENT DONNÉES
# ============================================================================

task_ex01_compile = BashOperator(
    task_id='ex01_compile_download_job',
    bash_command="""
    cd /opt/airflow/ex01_data_retrieval && \
    sbt clean package
    """,
    dag=dag,
)

task_ex01_copy_jar = BashOperator(
    task_id='ex01_copy_jar_to_spark',
    bash_command="""
    JAR_FILE="/opt/airflow/ex01_data_retrieval/target/scala-2.13/ex01_data_retrieval_2.13-0.1.0-SNAPSHOT.jar"
    docker cp "$JAR_FILE" spark-master:/home/root/
    echo "✓ JAR copied successfully"
    """,
    dag=dag,
)

task_ex01_download = BashOperator(
    task_id='ex01_download_to_minio',
    bash_command="""
    docker exec -e MINIO_ENDPOINT=minio:9000 \
        spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --class DirectDownload \
        --packages org.apache.hadoop:hadoop-aws:3.3.4, \
            software.amazon.awssdk:s3:2.17.100, \
            software.amazon.awssdk:aws-core:2.17.100, \
            software.amazon.awssdk:auth:2.17.100, \
            software.amazon.awssdk:regions:2.17.100 \
        /home/root/ex01_data_retrieval_2.13-0.1.0-SNAPSHOT.jar
    """,
    dag=dag,
)


# ============================================================================
# TASKS - EX02 : INGESTION SPARK
# ============================================================================

sync_point = DummyOperator(
    task_id='ex01_and_ex03_completed',
    dag=dag,
    trigger_rule='all_success'
)

task_ex02_compile = BashOperator(
    task_id='ex02_compile_spark_job',
    bash_command="""
    cd /opt/airflow/ex02_data_ingestion && \
    sbt clean package
    """,
    dag=dag,
)

task_ex02_copy_jar = BashOperator(
    task_id='ex02_copy_jar_to_spark',
    bash_command="""
    JAR_FILE="/opt/airflow/ex02_data_ingestion/target/scala-2.13/ex02_data_ingestion_2.13-0.1.0-SNAPSHOT.jar"
    docker cp "$JAR_FILE" spark-master:/home/root/
    echo "✓ JAR copied successfully"
    """,
    dag=dag,
)

task_ex02_spark_submit = BashOperator(
    task_id='ex02_spark_ingestion',
    bash_command="""
    docker exec -e MINIO_ENDPOINT=minio:9000 spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --class SparkApp \
        --packages org.apache.hadoop:hadoop-aws:3.3.4, \
        software.amazon.awssdk:s3:2.17.100, \
        software.amazon.awssdk:aws-core:2.17.100, \
        software.amazon.awssdk:auth:2.17.100, \
        software.amazon.awssdk:regions:2.17.100 \
        /home/root/ex02_data_ingestion_2.13-0.1.0-SNAPSHOT.jar
    """,
    dag=dag,
)


# ============================================================================
# TASKS - EX03 : TABLES SQL
# ============================================================================

task_ex03_create_tables = BashOperator(
    task_id='ex03_create_sql_tables',
    bash_command="""
    docker cp /opt/airflow/ex03_sql_table_creation postgres_db:/tmp/
    docker exec postgres_db psql -U mon_user -d ma_base -f /tmp/ex03_sql_table_creation/creation.sql
    """,
    dag=dag,
)

task_ex03_insert_data = BashOperator(
    task_id='ex03_insert_reference_data',
    bash_command="""
    docker exec postgres_db psql -U mon_user -d ma_base -f /tmp/ex03_sql_table_creation/insertion.sql
    """,
    dag=dag,
)


# ============================================================================
# TASKS - EX05 : MACHINE LEARNING
# ============================================================================

check_ml = BranchPythonOperator(
    task_id='check_ml_execution',
    python_callable=should_run_ml,
    dag=dag,
)

task_ex05_copy_code = BashOperator(
    task_id='ex05_copy_ml_code',
    bash_command="""
    docker cp /opt/airflow/ex05_ml_prediction_service/src spark-master:/home/root/
    """,
    dag=dag,
)

task_ex05_train_model = BashOperator(
    task_id='ex05_train_ml_model',
    bash_command="""
    docker exec -e MINIO_ENDPOINT=minio:9000 spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /home/root/src/model.py
    """,
    dag=dag,
)

skip_ml_training = DummyOperator(task_id='skip_ml_training', dag=dag)

task_launch_ml_ui = BashOperator(
    task_id='launch_ml_prediction_ui',
    bash_command="""
    echo "Lancement de l'interface ML sur le port 8501..."
    docker exec -d streamlit-page streamlit run /app/src/streamlit_page.py \
        --server.port 8501 --server.address 0.0.0.0
    sleep 5
    echo "✓ Interface de prédiction ML disponible sur: http://localhost:8501"
    """,
    dag=dag,
)


# ============================================================================
# TASK - FIN
# ============================================================================

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed_min_one_success'
    )


# ============================================================================
# DÉFINITION DES DÉPENDANCES
# ============================================================================

# Phase 1 : Ex01 (compile + copy + download) et Ex03 en parallèle
start >> [task_ex01_compile, task_ex03_create_tables]
task_ex01_compile >> task_ex01_copy_jar >> task_ex01_download

# Ex03 : création puis insertion des données de référence
task_ex03_create_tables >> task_ex03_insert_data

# Point de synchronisation : Ex02 attend que Ex01 ET Ex03 soient terminés
[task_ex01_download, task_ex03_insert_data] >> sync_point

# Phase 2 : Ex02 (compile + copy + ingestion Spark)
sync_point >> task_ex02_compile >> task_ex02_copy_jar >> task_ex02_spark_submit

# Phase 3 : Ex05 (Machine Learning)
task_ex02_spark_submit >> check_ml
check_ml >> [task_ex05_copy_code, skip_ml_training]
task_ex05_copy_code >> task_ex05_train_model

# Lancement de l'interface ML (après training)
task_ex05_train_model >> task_launch_ml_ui

# Fin du pipeline
[task_launch_ml_ui, skip_ml_training] >> end
