from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import os
from kafka import KafkaConsumer
from hdfs import InsecureClient

# --- CONFIGURATION DOCKER ---
KAFKA_HOST = 'kafka:9092'  
NAMENODE_URL = 'http://namenode:9870'
TOPIC = 'traffic-events'
HDFS_PATH = '/data/raw/traffic'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def consume_and_store_hdfs():
    print(10*"*"+"Démarrage Job Ingestion Kafka -> HDFS"+"*"*10)
    
    # 1. Connexion HDFS
    try:
        client = InsecureClient(NAMENODE_URL, user='root')
        print("✅ Connexion HDFS OK")
    except Exception as e:
        print(f"❌ Erreur HDFS: {e}")
        raise

    # 2. Consumer Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_HOST,
        group_id='airflow-hdfs-loader',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000, # Arrête de lire après 5s de silence
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []
    print(10*"*"+" Lecture des messages Kafka..."+"*"*10)
    
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 1000: # Batch max
            break
            
    if not messages:
        print("⚠️ Aucun nouveau message.")
        return

    # 3. Ecriture dans HDFS
    filename = f"traffic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    hdfs_file_path = f"{HDFS_PATH}/{filename}"
    
    # Convertir en NDJSON (Newline Delimited JSON) pour Spark
    content = "\n".join([json.dumps(msg) for msg in messages])
    
    with client.write(hdfs_file_path, encoding='utf-8') as writer:
        writer.write(content)
        
    print(10*"*"+f"💾 Sauvegardé : {hdfs_file_path} ({len(messages)} events)"+10*"*")


with DAG(
    'traffic_ingestion_kafka_hdfs',
    default_args=default_args,
    description='Ingestion Kafka vers HDFS',
    schedule_interval=timedelta(minutes=2), # Lance toutes les 2 minutes
    catchup=False,
) as dag:
    # 1. Tâche d'Ingestion (Kafka -> HDFS)
    ingest_task = PythonOperator(
        task_id='consume_kafka_write_hdfs',
        python_callable=consume_and_store_hdfs,
    )
    # 2. Tâche de Processing (Spark -> Postgres)
    process_task = BashOperator(
        task_id='trigger_spark_job',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --jars /opt/jobs/postgresql-42.7.3.jar \
          /opt/jobs/process_traffic.py
        """
    )
    # 3. Ordonnancement
    ingest_task >> process_task