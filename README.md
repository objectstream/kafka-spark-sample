Assumes the you have installed the following:

 Kafka is installed from : https://kafka.apache.org/quickstart
 
 Spark is installed : https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85
 
 Cassandra Installed : https://gist.github.com/hkhamm/a9a2b45dd749e5d3b3ae


 This example uses Scala for Spark.

Open the POM.xml using IntelliJ or Eclipse

Create Cassandra Table :
CREATE TABLE "dg_store"."dg_daily_store_sales" (
	 store_id text,
     timestamp_ms timestamp,
     timestamp_dt date,
     sales_total  text,
	PRIMARY KEY (store_id)
);

Run the cassandra_sink.scala file from the intellij IDE

On Terminal:
Start the Zookeeper from the the Kafka Unzipped/Untarred folder:
 bin/zookeeper-server-start.sh config/zookeeper.properties

Start the Kafka Server from the the Kafka Unzipped/Untarred folder:
 bin/kafka-server-start.sh config/server.properties
 
Create a topic "daily_sale"
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic daily_sales

Send Messages to Topic 'daily_sales'
{"timestamp_ms": "1530305100936", "store_id": "Hermitage - 1", "sales_total" :'1255.89'}



 
