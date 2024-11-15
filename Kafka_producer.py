from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load the Parquet file
file_path = '/Users/sk/Desktop/Projects/NYCTAXIDATA/NYCdata/yellow_tripdata_2024-01.parquet'
df = pd.read_parquet(file_path)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Publish each record as a Kafka message
topic = 'taxi_data'
for _, row in df.iterrows():
    producer.send(topic, row.to_dict())
    time.sleep(0.01)  # Simulate streaming delay