## Here is the article that mentions this : [Medium](https://medium.com/trending-information-technologies/stream-processing-17384a23111f)

### Assumes the you have installed the following:

 1. Kafka is installed from : https://kafka.apache.org/quickstart
 
 2. Spark is installed : https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85
 
 3. Cassandra Installed : https://gist.github.com/hkhamm/a9a2b45dd749e5d3b3ae

---
### To run the code follow these steps 

1. Create Cassandra Table :
`CREATE TABLE "dg_store"."dg_item_sales" (
     store_id text,
     timestamp_ms timestamp,
     timestamp_dt date,
     item_name  text,
     item_sold  text,
     item_id text,
     PRIMARY KEY (item_id)
);`

2. Import the POM.xml using IntelliJ or Eclipse

Run the KafkaToSparkToCassandra.scala file from the intellij IDE

On Terminal:
- Start the Zookeeper from the the Kafka Unzipped/Untarred folder:
 `bin/zookeeper-server-start.sh config/zookeeper.properties`

- Start the Kafka Server from the the Kafka Unzipped/Untarred folder:
 `bin/kafka-server-start.sh config/server.properties`
 
- Create a topic "item_sales"
`bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic item_sales`

- Send Messages to Topic 'item_sales', for example:
`{"item_id": "plates-235", "item_sold": 23, "item_name": "Plates", "timestamp_ms": "1530305100936", "store_id": "Hermitage - 1"}`


3. You should see the message getting saved to Cassandra DB / table




 
