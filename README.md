# Realtime Product Data Pipeline

## Overview

This project implements a real-time product data pipeline using MySQL,
Python, Apache Kafka, and Apache Avro.

The pipeline incrementally reads newly added or updated product records
from MySQL, serializes them using Avro, publishes them to a Kafka topic,
and processes them using a five-consumer group. The consumers transform
the data and store the results as JSON files.

## Architecture

MySQL
↓
Python Kafka Producer
↓
Avro Serialization
↓
Kafka: product_updates
↓
5 Consumers
↓
Data Transformation
↓
JSON Output Files

## Technologies

- Python
- MySQL
- Apache Kafka
- Apache Avro
- kafka-python
- fastavro
- mysql-connector-python
- Docker

## Project Structure

realtime-product-data-pipeline/
│
├── consumer/
├── output/
├── producer/
├── schemas/
├── screenshots/
├── sql/
├── .gitignore
├── README.md
└── requirements.txt

## MySQL Setup

Create the database:

```sql
CREATE DATABASE kafkaproj;
USE kafkaproj;
```

Create the table:

```sql
CREATE TABLE product (
    id INT,
    product_name VARCHAR(100),
    category VARCHAR(100),
    price FLOAT,
    last_updated TIMESTAMP,
    CONSTRAINT pk_product PRIMARY KEY(id)
);
```

## Kafka Topic

The Kafka topic used in the project is:

product_updates

The topic is configured with 10 partitions and replication factor 1.

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic product_updates --bootstrap-server localhost:9092 --partitions 10 --replication-factor 1

The topic can be verified using:

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic product_updates --bootstrap-server localhost:9092

The product ID is used as the Kafka message key, ensuring that updates for the same product are sent to the same partition.

Incremental Data Extraction

The producer maintains the timestamp of the last processed record in:

producer/last_timestamp.txt

The producer fetches only records newer than the stored timestamp:

SELECT \*
FROM product
WHERE last_updated > %s
ORDER BY last_updated;

After successful publishing, the checkpoint is updated to the latest processed timestamp. This prevents previously processed records from being fetched repeatedly.

# Avro Serialization

Product records are serialized using the Avro schema stored in:

schemas/product.avsc

The schema contains:

id
product_name
category
price
last_updated

The producer serializes each product using fastavro before publishing it to Kafka.

Kafka Producer

The producer performs the following operations:

Reads the last processed timestamp.
Connects to MySQL.
Fetches new or updated records.
Converts the records into the Avro format.
Publishes the Avro data to product_updates.
Uses the product ID as the Kafka message key.
Updates the timestamp checkpoint after successful publishing.
Kafka Consumer Group

The project uses the consumer group:

avro_product_consumer_group

Five consumer instances are started using:

python consumer.py 1
python consumer.py 2
python consumer.py 3
python consumer.py 4
python consumer.py 5

All consumers belong to the same consumer group, allowing Kafka to distribute the topic partitions among the active consumers.

# Data Transformation

After consuming and deserializing the Avro message, each consumer applies the following transformations:

Converts the product category to uppercase.
Applies a 10% discount to products in the Electronics category.

Example:

Before:
Category = Electronics
Price = 4000

After:
Category = ELECTRONICS
Price = 3600
JSON Output

Each consumer writes its processed records to a separate JSON file:

output/
├── consumer_1.json
├── consumer_2.json
├── consumer_3.json
├── consumer_4.json
└── consumer_5.json

The files use append mode, with one JSON object stored per line.

Example:

{"id":20,"product_name":"Bluetooth Headphones","category":"ELECTRONICS","price":3600.0,"last_updated":"2026-09-19 14:00:00"}
Running the Project

1. Start Kafka

Ensure the Kafka Docker container is running.

2. Start the five consumers

From the consumer directory:

python consumer.py 1
python consumer.py 2
python consumer.py 3
python consumer.py 4
python consumer.py 5 3. Insert new data into MySQL
INSERT INTO product
(id, product_name, category, price, last_updated)
VALUES
(20, 'Bluetooth Headphones', 'Electronics', 4000, CURRENT_TIMESTAMP); 4. Run the producer

From the producer directory:

python producer.py

The producer detects the new record, serializes it using Avro, and publishes it to Kafka. The consumer group then processes the message, applies the transformations, and stores the result in the corresponding JSON output file.
