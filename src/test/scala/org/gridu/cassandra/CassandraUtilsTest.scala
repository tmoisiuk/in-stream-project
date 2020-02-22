package org.gridu.cassandra

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.gridu.config.CassandraConfig
import org.gridu.spark.EnrichedClick
import org.scalatest.funsuite.AnyFunSuite

class CassandraUtilsTest extends AnyFunSuite {

  ignore("write to Cassandra") {

    val config = CassandraConfig("0.0.0.0", "9042", "botdetection", "enrichedclicks")

    lazy val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("test_write")
      .config("spark.cassandra.connection.host", config.host)
      .config("spark.cassandra.connection.port", config.port)
      .getOrCreate()

    val rdd: RDD[EnrichedClick] = spark.sparkContext.parallelize(
      Seq(
        EnrichedClick("click", "10.186.10.10", is_bot = true, 123123, "http://"),
        EnrichedClick("click", "10.186.10.11", is_bot = true, 123123, "http://")
      )
    )

    CassandraUtils.writeToCassandra(rdd, config)
  }
}
