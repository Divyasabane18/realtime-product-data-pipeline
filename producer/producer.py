import mysql.connector
from kafka import KafkaProducer
import json

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
cursor.execute("select * from product")

rows =cursor.fetchall()

producer = KafkaProducer(
    bootstrap_servers= "localhost:9092",
    value_serializer= lambda x: json.dumps(x).encode("utf-8")
)

#sqls each product is been sent to kafka
for row in rows:
    product ={
        "id" : row[0],
        "product_name" : row[1],
        "category" : row[2],
        "price": float(row[3]),
        "last_update":str(row[4])
    }

    producer.send("Producer_topic", value=product)


producer.flush()

#close connection
cursor.close()
connection.close()
print("All products sent to kafka")