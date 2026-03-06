DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS complaints;

CREATE TABLE customers (
                           id        INT PRIMARY KEY,
                           name      VARCHAR(50),
                           region    VARCHAR(16),
                           status    VARCHAR(16)
);

CREATE TABLE orders (
                        id          INT PRIMARY KEY,
                        customer_id INT,
                        amount      DECIMAL(10,2),
                        status      VARCHAR(16),
                        order_date  DATE
);

CREATE TABLE products (
                          id       INT PRIMARY KEY,
                          category VARCHAR(16),
                          price    DECIMAL(10,2)
);

CREATE TABLE order_items (
                             order_id   INT,
                             product_id INT,
                             quantity   INT
);

CREATE TABLE complaints (
                            id          INT PRIMARY KEY,
                            customer_id INT,
                            severity    INT
);

INSERT INTO customers(id, name, region, status) VALUES
                                                    (1, 'Alice', 'EAST',  'ACTIVE'),
                                                    (2, 'Bob',   'WEST',  'INACTIVE'),
                                                    (3, 'Carol', 'EAST',  'ACTIVE'),
                                                    (4, 'Dave',  'NORTH', 'ACTIVE');

INSERT INTO orders(id, customer_id, amount, status, order_date) VALUES
                                                                    (1, 1,  50.00,  'OPEN',   DATE '2024-01-01'),
                                                                    (2, 1, 250.00,  'OPEN',   DATE '2024-02-01'),
                                                                    (3, 2, 300.00,  'CLOSED', DATE '2024-03-01'),
                                                                    (4, 3, 150.00,  'OPEN',   DATE '2024-04-01'),
                                                                    (5, 4, 500.00,  'OPEN',   DATE '2024-05-01');

INSERT INTO products(id, category, price) VALUES
                                              (1, 'ELEC', 100.00),
                                              (2, 'TOY',   20.00),
                                              (3, 'ELEC', 200.00);

INSERT INTO order_items(order_id, product_id, quantity) VALUES
                                                            (1, 1, 1),
                                                            (2, 3, 2),
                                                            (3, 2, 1),
                                                            (4, 1, 3),
                                                            (5, 2, 4);

INSERT INTO complaints(id, customer_id, severity) VALUES
                                                      (1, 1, 1),
                                                      (2, 4, 2);

SELECT 'T01' AS t, o.id AS order_id
FROM orders o
         JOIN customers c ON o.customer_id = c.id
WHERE c.id = 1
ORDER BY o.id;

SELECT 'T02' AS t, c.id AS customer_id
FROM orders o
         JOIN customers c ON o.customer_id = c.id
WHERE o.customer_id = 3
ORDER BY c.id;

SELECT 'T03' AS t, o.id AS order_id, c.id AS customer_id
FROM orders o
         JOIN customers c  ON o.customer_id = c.id
         JOIN complaints m ON m.customer_id = c.id
WHERE m.customer_id = 4
ORDER BY o.id;

SELECT 'T04' AS t, c.id AS customer_id
FROM customers c
         LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.amount > 100
ORDER BY c.id;

SELECT 'T05' AS t, c.id AS customer_id
FROM customers c
WHERE c.id IN (
    SELECT o.customer_id
    FROM orders o
    WHERE o.status = 'OPEN' AND o.amount > 200
)
ORDER BY c.id;

SELECT 'T06' AS t, c.id AS customer_id
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.id
      AND o.amount BETWEEN 100 AND 300
)
ORDER BY c.id;

SELECT 'T07' AS t, o.id AS order_id
FROM (
         SELECT id, customer_id FROM orders WHERE status = 'OPEN'
     ) o
         JOIN customers c ON o.customer_id = c.id
WHERE c.region = 'EAST'
ORDER BY o.id;

SELECT 'T08' AS t, o.id AS order_id
FROM orders o
         JOIN customers c ON o.customer_id = c.id
WHERE c.id = 1 OR c.id = 3
ORDER BY o.id;

SELECT 'T09' AS t, c.id AS customer_id
FROM customers c
         JOIN (SELECT DISTINCT customer_id FROM orders) o
              ON o.customer_id = c.id
WHERE c.status = 'ACTIVE'
ORDER BY c.id;

SELECT 'T10' AS t, o.id AS order_id
FROM customers c
         JOIN (SELECT * FROM orders WHERE amount > 200) o
              ON o.customer_id = c.id
WHERE c.status = 'ACTIVE'
ORDER BY o.id;
