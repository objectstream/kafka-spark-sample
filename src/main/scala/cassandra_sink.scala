package com.insight.app.CassandraSink

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr

class CassandraSinkForeach() extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called 
  // whenever there is a sequence of rows generated as output

  var cassandraDriver: CassandraDriver = null;
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    if (cassandraDriver == null) {
      cassandraDriver = new CassandraDriver();
    }
    cassandraDriver.connector.withSessionDo(session =>
      session.execute(s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (store_id, timestamp_ms, sales_total, timestamp_dt)
       values('${record(0)}', '${record(1)}', '${record(2)}', '${record(3)}')""")
        //Process new [Hermitage - 1,1530305100936,1255.89,2018-06-29]

    )
  }
  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors. 
  // Note here the usage of @transient lazy val 
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
    .setAppName("Structured Streaming from Kafka to Cassandra")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .setMaster("local")

    @transient lazy val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

    spark
  }
}

class CassandraDriver extends SparkSessionBuilder {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
  val spark = buildSparkSession
  
  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)
  
  // Define Cassandra's table for saving the messages from Kafka -> Spark --> Cassanda
  /* For this app I used the following table:
      CREATE TABLE "dg_store"."dg_daily_store_sales" (
     store_id text,
     timestamp_ms timestamp,
     timestamp_dt date,
     sales_total  text,
	  PRIMARY KEY (store_id)
);
  */
  val namespace = "dg_store"
  val foreachTableSink = "dg_daily_store_sales"
}

object KafkaToCassandra extends SparkSessionBuilder {
  // Main body of the app. It also extends SparkSessionBuilder.
  def main(args: Array[String]) {
    val spark = buildSparkSession
  
    import spark.implicits._
    
    // Define location of Kafka brokers:
    val broker = "localhost:9092"

    /*Here is an example massage which I get from a Kafka stream. It contains multiple jsons separated by \n 
    {"timestamp_ms": "1530305100936", "store_id": "Hermitage - 1", "sales_total" :'1255.89'}
   
    */
    val dfraw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", "daily_sales")
      .option("failOnDataLoss", false)

      .load()

    val schema = StructType(
      Seq(
        StructField("store_id", StringType, false),
        StructField("timestamp_ms", StringType, false),
        StructField("sales_total", StringType, false)
      )
    )

    val df = dfraw
    .selectExpr("CAST(value AS STRING)").as[String]
    .flatMap(_.split("\n"))

    val jsons = df.select(from_json($"value", schema) as "data").select("data.*")

    val parsed = jsons
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
      .filter("store_id != ''")

    val sink = parsed
    .writeStream
    .queryName("KafkaToCassandraForeach")
    .outputMode("update")
    .foreach(new CassandraSinkForeach())
    .start()

    sink.awaitTermination()
  }
}
