package org.gridu

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

object ParkReader extends App {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("parquet_loader")
    .getOrCreate()

  import spark.implicits._


  val ds = spark.read.parquet("hdfs://hadoop-namenode02.prod.data-platform.wz-ams.lo.mobbtech.com/tmp/perf_test/iqbus_volatility-volatility-generated-2_0-raw/*").as[RawKafkaRecord]
  //  val ds = spark.read.parquet("/Users/taras.moysyuk/IdeaProjects/kafka_hdfs_loader/tmp/iqbus_volatility-volatility-generated-2_0-raw/*").as[RawKafkaRecord]

  ds.createOrReplaceTempView("messages")
  spark.sql("select * from (select offset, count(*) as cnt from messages group by offset) where cnt > 1").show()

  ds.toDF.agg(min("timestamp")).show()
  ds.toDF.agg(max("timestamp")).show()
  println("count: " + ds.count())
}

case class RawKafkaRecord(key: String, value: String, timestamp: Long, offset: Long, partition: Int, topic: String)
