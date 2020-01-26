package org.gridu.config

import com.typesafe.config.ConfigFactory

import pureconfig.generic.auto._


case class AppConfig(spark: SparkConfig, kafka: KafkaConfig, botConfig: BotConfig)

case class SparkConfig(appName: String, mode: String)

case class KafkaConfig(topic: String, params: Map[String, String])

case class BotConfig(
                      botMessagesNumber: Int,
                      botTimeThreshold: Long,
                      batchDuration: Long,
                      windowDuration: Long,
                      slideDuration: Long,
                    )

object AppConfig {
  def apply(): AppConfig = {

    val config = ConfigFactory.load("pure.conf")
    pureconfig.loadConfigOrThrow[AppConfig](config)
  }
}
