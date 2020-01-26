package org.gridu.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gridu.config.KafkaConfig

import scala.collection.JavaConverters._

trait StructuredStreamProvider {
  def streamingDataset: Dataset[(String, String)]
}

class KafkaStructuredStreamProvider(spark: SparkSession,
                                    kafkaConfig: KafkaConfig) extends StructuredStreamProvider {
  override def streamingDataset: Dataset[(String, String)] = {

    import spark.implicits._
    spark
      .readStream
      .format("kafka")
      .options(kafkaConfig.params.mapValues(_.toString).asJava)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }
}
