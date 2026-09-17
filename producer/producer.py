import mysql.connector
from kafka import KafkaProducer
import json
from datetime import datetime
from fastavro import parse_schema,schemaless_writer
from io import BytesIO


with open("../schemas/product.avsc", "r") as file:
    schema = json.load(file)

parsed_schema =parse_schema(schema)

print("Avro schema loaded successfully !")

with open("last_timestamp.txt", "r") as file:
    last_read_timestamp = datetime.fromisoformat(file.read().strip())
    print("Last read timestamp:", last_read_timestamp )


#connection to mysql
connection = mysql.connector.connect(
    host= "localhost",
    user = "root",
    password ="divya18",
    database= "kafkaproj"
)

print("Connectred to mysql successfully")

#create cursor
#A cursor is basically what Python uses to send SQL commands to MySQL and retrieve the results.
cursor =connection.cursor()

#fetch products
#this select * from product fetch everything but we want to do using lasttimestamp
cursor.execute("""select * from product where last_updated > %s order by last_updated""" , (last_read_timestamp,))

rows =cursor.fetchall()

producer = KafkaProducer(
    bootstrap_servers= "localhost:9092",
    key_serializer = lambda key: str(key).encode("utf-8"),
    value_serializer= lambda value: value
)

#sqls each product is been sent to kafka

if rows:
    for row in rows:
        product ={
            "id" : row[0],
            "product_name" : row[1],
            "category" : row[2],
            "price": float(row[3]),
            "last_updated":str(row[4])
        }

    #this creating a temporary place in memory to store bytes
    buffer = BytesIO()
    print("Product being serialized:", product)
    schemaless_writer(buffer,parsed_schema,product)

    # the above line of code means "Take my product dictionary and serialize it into Avro binary format using parsed_schema, then put the result into buffer"

    avro_data = buffer.getvalue()

    print("Avro serialization successfull !")
    print("Avro data: ", avro_data)

    producer.send("product_updates",key=product["id"], value=avro_data)


producer.flush()

if rows:
    max_timestamp = max(row[4] for row in rows)

    with open("last_timestamp.txt","w") as file:
        file.write(max_timestamp.isoformat(sep=" "))
        print("Update last read timestamp:" , max_timestamp)

#close connection
cursor.close()
connection.close()
print("All products sent to kafka")