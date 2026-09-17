from kafka import KafkaConsumer
import json 

consumer = KafkaConsumer(
    "Producer_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="product_consumer_group",
    value_deserializer=lambda x:json.loads(x.decode("utf-8"))
)

print("Connected to kafka topic")

for message in consumer:
    print(message.value)