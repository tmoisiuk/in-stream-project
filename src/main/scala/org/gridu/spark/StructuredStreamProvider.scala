package org.gridu.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gridu.config.KafkaConfig

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
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers.mkString(","))
      .option("subscribe", kafkaConfig.topic)
      .option("startingOffsets", kafkaConfig.autoOffsetReset)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }
}
