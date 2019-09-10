package com.objectstream.app.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionFactory extends Serializable {
 // @transient lazy val pattern comes in.
 // In Scala lazy val denotes a field that will only be calculated once it is accessed for the first time and is
 // then stored for future reference.

  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Spark Streaming from Kafka to cassandra")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .setMaster("local")

    @transient lazy val aSparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    aSparkSession
  }
}