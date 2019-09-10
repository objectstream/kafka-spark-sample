package com.objectstream.app.sparkstreaming



import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions._
//

object ToCassandraFromKafkaViaSpark extends SparkSessionFactory {

  //Extending the SparkSessionFactory
  def main(args: Array[String]) {
    val spark = buildSparkSession

    import spark.implicits._

    // localhost kafka server
    val broker = "localhost:9092"

    /*

    {"item_id": "plates-235", "item_sold": 23, "item_name": "Plates", "timestamp_ms": "1530305100936", "store_id": "Hermitage - 1"}

    */

    val dframe = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", "item_sales")
      .option("failOnDataLoss", false)
      .load()

    dframe.printSchema() // returns the schema of Kafka streaming


    val schema = new StructType()
      .add("item_id",StringType)
      .add("item_sold",StringType)
      .add("item_name",StringType)
      .add("timestamp_ms",StringType)
      .add("store_id",StringType)
     

    val itemDF = dframe
    .selectExpr("CAST(value AS STRING)").as[String]
    .flatMap(_.split("\n"))

    val itemJson = itemDF.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    val parsedItemJson = itemJson
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
      .filter("item_id != ''")

    val sink = parsedItemJson
    .writeStream
    .queryName("KafkaToCassandraForeach")
    .outputMode("update")
    .foreach(new PersistSparkStreamRow())
    .start()

    sink.awaitTermination()
  }
}
