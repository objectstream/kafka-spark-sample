
package com.objectstream.app.sparkstreaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.objectstream.app.sparkstreaming.SparkSessionFactory

class CassandraPersistor extends SparkSessionFactory {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
  val spark = buildSparkSession

  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)

  // Define Cassandra's table for saving the messages from Kafka -> Spark --> Cassanda
  /* For this app I used the following table:
     CREATE TABLE "dg_store"."dg_item_sales" (
     store_name text,
     timestamp_ms timestamp,
     timestamp_dt date,
     item_name  text,
     item_cost  float,
     id uuid,
     PRIMARY KEY (id)
);
  */
  //This is the cassandra namespace and
  val namespace = "dg_store"
  //val foreachTableSink = "dg_daily_store_sales"
  val foreachTableSink = "dg_item_sales"
}
