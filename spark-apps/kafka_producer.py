# spark-apps/kafka_producer.py
from kafka import KafkaProducer
import json
from traffic_data_generator import generate_traffic_event

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoyer des événements en continu