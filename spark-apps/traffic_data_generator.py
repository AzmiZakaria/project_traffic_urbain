import json
import random
from datetime import datetime, timedelta

# Définir vos zones
ZONES = ["Centre-ville", "Nord", "Sud", "Est", "Ouest"]
ROAD_TYPES = ["autoroute", "avenue", "rue"]

def generate_traffic_event(sensor_id, hour):
    # Heures de pointe: 7-9h et 17-19h
    is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
    
    return {
        "sensor_id": f"SENSOR_{sensor_id:03d}",
        "road_id": f"ROAD_{random.choice(['A1', 'B2', 'C3'])}",
        "road_type": random.choice(ROAD_TYPES),
        "zone": random.choice(ZONES),
        "vehicle_count": random.randint(50, 200) if is_rush_hour else random.randint(10, 50),
        "average_speed": random.uniform(30, 80) if not is_rush_hour else random.uniform(10, 40),
        "occupancy_rate": random.uniform(60, 95) if is_rush_hour else random.uniform(20, 50),
        "event_time": datetime.now().isoformat()
    }