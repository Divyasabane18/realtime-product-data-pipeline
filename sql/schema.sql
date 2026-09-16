CREATE DATABASE kafkaproj;

USE kafkaproj;

CREATE TABLE product (
    id INT,
    product_name VARCHAR(100),
    category VARCHAR(100),
    price FLOAT,
    last_updated TIMESTAMP,
    CONSTRAINT pk_product PRIMARY KEY(id)
);

INSERT INTO product
(id, product_name, category, price)
VALUES
(1, 'Laptop', 'Electronics', 55000),
(2, 'Wireless Mouse', 'Electronics', 800),
(3, 'Office Chair', 'Furniture', 4500),
(4, 'Keyboard', 'Electronics', 1500),
(5, 'Desk Lamp', 'Home', 1200);