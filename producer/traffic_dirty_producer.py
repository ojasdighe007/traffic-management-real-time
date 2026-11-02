from kafka import KafkaProducer
from faker import Faker
import json 
import random 
import time 
from datetime import datetime, timedelta
import pytz 

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
     value_serializer=lambda v:json.dumps(v).encode("utf-8") # passes the value v which is to be serialized into json.dumps function which maps a python dict to a JSON string (which is serializable)
    # UTF stands for Unicode Transformation Format.
    # It is a standard character encoding system that converts universal text characters into binary byte sequences. Since strings cannot be streamed, we convert them into bytes using UTF-8 in this case. 
    # Then at the receiver side, these will be decoded using utf-8 to get the values.

    # Common Types of UTF

    # • UTF-7: Uses 7-bit ASCII encoding, mainly designed for email transmission.
    # • UTF-8: Uses 1 to 4 bytes per character, is backward-compatible with ASCII, and is the dominant text encoding on the internet.
    # • UTF-16: Uses 2 or 4 bytes per character to represent Unicode code points.
    # • UTF-32: Uses a fixed 4 bytes for every single character.
)

roads = ["R100", "R200", "R300", "R400"]
zones = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
weather = ["CLEAR", "RAIN", "FOG", "STORM"]

vehicle_cache = []

def generate_clean_event():

    vid = fake.uuid4()
    vehicle_cache.append(vid)

    return {
        "vehicle_id": vid,
        "road_id": random.choice(roads),
        "city_zone": random.choice(zones),
        "speed": random.randint(20, 100),
        "congestion_level": random.randint(1,5), # Generates integer values only
        "weather": random.choice(weather),
        "event_time": datetime.now(pytz.utc).isoformat()
    }


def generate_dirty_event():

    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None

    elif dirty_type == "negative_speed":
        base["speed"] = -40

    elif dirty_type == "extreme_speed":
        base["speed"] = 420

    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)

    elif dirty_type == "late_event":
        base["event_time"] = (
            datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))
        ).isoformat()

    elif dirty_type == "future_event":
        base["event_time"] = (
            datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))
        ).isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed"] = "FAST"

    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])

    elif dirty_type == "corrupt_json":
        return "###CORRUPTED_EVENT###"

    return base

while True:

    if random.random() < 0.7: # Generates float numbers between 0 to 1. So expected probability of clean event is 70%, rest 30% is bad events.
        event = generate_clean_event()
    else:
        event = generate_dirty_event()
    
    if isinstance(event, str):
        producer.send("traffic-topic",  value={"raw": event})
        print("CORRUPT EVENT SENT")
    else:
        producer.send("traffic-topic", value=event)
        print(event)

    time.sleep(random.uniform(0.5,1.5)) # same as random.random() except that it accepts custom range (generates float numbers too)


    

    