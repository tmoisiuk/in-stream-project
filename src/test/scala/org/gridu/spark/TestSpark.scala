package org.gridu.spark

import org.apache.spark.sql.SparkSession

trait TestSpark {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test-app")
    .config("spark.cassandra.connection.host", "0.0.0.0")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
}