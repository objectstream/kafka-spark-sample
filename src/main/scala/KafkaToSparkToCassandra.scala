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

    /*Here is an example massage which I get from a Kafka stream. It contains multiple jsons separated by \n
    {"timestamp_ms": "1530305100936", "store_id": "Hermitage - 1", "sales_total" :'1255.89'}

    {"timestamp_ms": "1530305100936", "store_id": "Hermitage - 1", "sales_total" :'1255.89'}

    {"item_id": "plates-234", "item_cost": 23.57, "item_name": "Plates", "timestamp_ms": "1530305100936", "store_id": "Hermitage - 1"}
  {"item_id": "plates-235", "item_sold": 23, "item_name": "Plates", "timestamp_ms": "1530305100936", "store_id": "Hermitage - 1"}

    */

    val dfraw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", "item_sales")
      .option("failOnDataLoss", false)
      .load()

    dfraw.printSchema() // returns the schema of Kafka streaming
    
    /*val schema = StructType(
      Seq(
        StructField("store_id", StringType, false),
        StructField("timestamp_ms", StringType, false),
        StructField("sales_total", StringType, false)
      )
    )*/


    val schema = new StructType()
      .add("item_id",StringType)
      .add("item_sold",StringType)
      .add("item_name",StringType)
      .add("timestamp_ms",StringType)
      .add("store_id",StringType)
     
    
    // Using a struct
   // val schema = new StructType().add("a", new StructType().add("b", IntegerType))

    val df = dfraw
    .selectExpr("CAST(value AS STRING)").as[String]
    .flatMap(_.split("\n"))

    //val jsons = df.select(from_json($"value", schema) as "data").select("data.*")

    val jsons = df.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    val parsed = jsons
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))

      .filter("item_id != ''")

   // val df2 = df.withColumn("val_float", $"value".cast(FloatType))

    val sink = parsed
    .writeStream
    .queryName("KafkaToCassandraForeach")
    .outputMode("update")
    .foreach(new PersistSparkStreamRow())
    .start()

    sink.awaitTermination()
  }
}
