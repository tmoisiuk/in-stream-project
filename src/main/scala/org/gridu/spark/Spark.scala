package org.gridu.spark

import org.apache.spark.sql.SparkSession
import org.gridu.config.AppConfig

trait Spark {
  val appConfig: AppConfig

  lazy val spark: SparkSession = SparkSession.builder()
    .master(appConfig.spark.mode)
    .appName(appConfig.spark.appName)
    .getOrCreate()

}