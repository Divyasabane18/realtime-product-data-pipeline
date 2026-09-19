from kafka import KafkaConsumer
import json 
from fastavro import parse_schema, schemaless_reader
from io import BytesIO
import sys 

consumer_id = sys.argv[1]
output_file = f"../output/consumer_{consumer_id}.json"
print("Output file:", output_file)
print("Consumer ID: " ,consumer_id)


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

    #tranform data 
    product["category"] = product["category"].upper()

    if product["category"] == "ELECTRONICS":
        product["price"] = round(product["price"] * 0.90 ,2)
        

    print("Received product: ", product)
    with open(output_file, "a", encoding="utf-8") as file:
        file.write(json.dumps(product)+ "\n")