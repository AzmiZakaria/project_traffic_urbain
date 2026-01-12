import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# CONFIGURATION KAFKA
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093' 
KAFKA_TOPIC = 'traffic-events'


def generate_event():
    """Génère un événement de trafic simulé"""
    vehicle_count = random.randint(0, 150)
    
    if vehicle_count > 100:
        avg_speed = random.randint(5, 20)
        occupancy = random.randint(80, 100)
    elif vehicle_count > 50:
        avg_speed = random.randint(20, 50)
        occupancy = random.randint(40, 80)
    else:
        avg_speed = random.randint(50, 90)
        occupancy = random.randint(0, 40)

    event = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": f"sens-{random.randint(1, 20)}",
        "road_id": f"rd-{random.randint(100, 110)}",
        "road_type": random.choice(['Autoroute', 'Boulevard', 'Rue', 'Avenue']),
        "zone": random.choice(['Centre-Ville', 'Zone-Industrielle', 'Banlieue-Nord', 'Banlieue-Sud']),
        "vehicle_count": vehicle_count,
        "average_speed": avg_speed,
        "occupancy_rate": occupancy,
        "timestamp": datetime.now().isoformat()
    }
    return event

def main():
    print(f"Démarrage Générateur -> Kafka ({KAFKA_BOOTSTRAP_SERVERS})")
    
    # Tentative de connexion au broker Kafka
    try:
        # Création du producteur Kafka avec sérialisation JSON
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Conversion dict -> JSON -> bytes
        )
        print("✅ Connecté à Kafka !")
    except Exception as e:
        print(f"❌ Erreur connexion Kafka (Vérifie que le port 9093 est ouvert) : {e}")
        return

    # Boucle principale d'envoi des événements
    try:
        while True:
            # Génération d'un nouvel événement de trafic
            event = generate_event()
            
            # Envoi de l'événement au topic Kafka
            producer.send(KAFKA_TOPIC, value=event)
            
            # Affichage des informations de l'événement envoyé
            print(f"📤 [Sensor {event['sensor_id']}] Vitesse: {event['average_speed']} km/h | Zone {event['zone']} | Véhicules {event['vehicle_count']}")
            
            # Pause d'1 seconde entre chaque envoi (1 message/seconde)
            time.sleep(1)
            
    except KeyboardInterrupt:
        # Gestion de l'arrêt propre avec Ctrl+C
        print("")
        print(10*"*"+" Arrêt du générateur. "+"*"*10)
        producer.close()  # Fermeture propre de la connexion Kafka

if __name__ == "__main__":
    main()