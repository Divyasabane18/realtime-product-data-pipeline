from kafka import KafkaConsumer
import json 
from fastavro import parse_schema, schemaless_reader
from io import BytesIO

with open("../schemas/product.avsc", "r") as file:
    schema = json.load(file)

    parsed_schema = parse_schema(schema)

    print("Avro schema loaded successfully !")

consumer = KafkaConsumer(
    "product_updates",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="avro_product_consumer_group",
    
)

print("Connected to kafka topic")

for message in consumer:
    buffer = BytesIO(message.value)
    product = schemaless_reader(buffer, parsed_schema)
    print("Received product: ", product)