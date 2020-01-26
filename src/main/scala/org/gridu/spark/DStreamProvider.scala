package org.gridu.spark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.gridu.config.KafkaConfig

trait DStreamProvider {
  def stream: DStream[ConsumerRecord[String, String]]
}

class KafkaDStreamProvider(ssc: StreamingContext,
                           kafkaConfig: KafkaConfig) extends DStreamProvider {
  override def stream: DStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Seq(kafkaConfig.topic), kafkaConfig.params))
}
