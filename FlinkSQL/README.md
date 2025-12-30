#Flink SQL Code

## This is the code required to run the other apps.

After creating the docker containers and copying the required Kafka connectors into both the jobmanager and the taskmanager, These steps are required to run the programms.

Before starting the Kafka topics need to be created manually. This is achieved easily by navigating to port 9000 (Kafdrop, Kafka UI) and creating the topics there by clicking new on the bottom left corner.

The required topics are :

- user-events
- users
- cart-events
- agg-user-items
- agg-user-cart
- agg-total-cart
- agg-product-sales


After creating the topic and copying the Kafka connectors severed SQL tables need to be created and data must be inserted into them. In the Jobmanager execution environment the following commands and SQL statements in order can be used.

./bin/start-cluster.sh

./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost

./bin/sql-client.sh gateway --endpoint http://localhost:8083

CREATE TABLE cart_events (
  userId BIGINT,
  userName STRING,
  productId BIGINT,
  name STRING,
  unitPrice DOUBLE,
  delta INT,
  action STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'cart-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-cart',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);


CREATE TABLE agg_total_cart (
  metric STRING,
  total_price DOUBLE,
  total_items BIGINT,
  PRIMARY KEY (metric) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'agg-total-cart',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);


CREATE TABLE agg_user_cart (
  userId BIGINT,
  userName STRING,
  items BIGINT,
  total_price DOUBLE,
  distinct_products BIGINT,
  PRIMARY KEY (userId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'agg-user-cart',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);


CREATE TABLE agg_product_sales (
  productId BIGINT,
  name STRING,
  sold_count BIGINT,
  revenue DOUBLE,
  PRIMARY KEY (productId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'agg-product-sales',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);


INSERT INTO agg_total_cart
SELECT
  'all' AS metric,
  SUM(unitPrice) AS total_price,
  COUNT(*) AS total_items
FROM cart_events;


CREATE TABLE agg_user_items (
  userId BIGINT,
  productId BIGINT,
  userName STRING,
  name STRING,
  qty BIGINT,
  subtotal DOUBLE,
  PRIMARY KEY (userId, productId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'agg-user-items',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);


CREATE VIEW user_items AS
SELECT
  userId,
  productId,
  MAX(userName) AS userName,
  MAX(name) AS name,
  SUM(delta) AS qty,
  SUM(delta * unitPrice) AS subtotal
FROM cart_events
GROUP BY userId, productId
HAVING SUM(delta) > 0;


INSERT INTO agg_user_items
SELECT * FROM user_items;


INSERT INTO agg_user_cart
SELECT
  userId,
  MAX(userName) AS userName,
  SUM(qty) AS items,
  SUM(subtotal) AS total_price,
  COUNT(*) AS distinct_products
FROM user_items
GROUP BY userId;


INSERT INTO agg_product_sales
SELECT
  productId,
  MAX(name) AS name,
  SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END) AS sold_count,
  SUM(CASE WHEN delta > 0 THEN delta * unitPrice ELSE 0 END) AS revenue
FROM cart_events
GROUP BY productId;


CREATE TABLE user_events (
  userId BIGINT,
  userName STRING,
  eventType STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-users',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);


CREATE TABLE users (
  userId BIGINT,
  userName STRING,
  PRIMARY KEY (userId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);


INSERT INTO users
SELECT userId, userName
FROM (
  SELECT
    userId,
    MAX(userName) AS userName,
    MAX(CASE WHEN eventType = 'DELETE' THEN 1 ELSE 0 END) AS is_deleted
  FROM user_events
  GROUP BY userId
)
WHERE is_deleted = 0;
