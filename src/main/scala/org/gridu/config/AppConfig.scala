package org.gridu.config

import com.typesafe.config.ConfigFactory
import pureconfig.generic.auto._

import scala.io.Source


case class AppConfig(
                      spark: SparkConfig,
                      kafka: KafkaConfig,
                      botConfig: BotConfig,
                      cassandra: CassandraConfig
                    )

case class SparkConfig(appName: String, mode: String)


case class KafkaConfig(
                        topic: String,
                        bootstrapServers: Seq[String],
                        groupId: String,
                        autoOffsetReset: String,
                        enableAutoCommit: Boolean
                      )

case class CassandraConfig(
                            host: String,
                            port: String,
                            keySpace: String,
                            table: String
                          )

case class BotConfig(
                      botMessagesNumber: Int,
                      botTimeThreshold: Long,
                      batchDuration: Long,
                      windowDuration: Long,
                      slideDuration: Long,
                    )

object AppConfig {

  def apply(reference: String = ""): AppConfig = {

    val conf = if (reference.nonEmpty) ConfigFactory.parseString(getTextFileContent(reference))
      else ConfigFactory.load("pure.conf")

    pureconfig.loadConfigOrThrow[AppConfig](conf)
  }

  private def getTextFileContent(path: String): String = {
    val resourceInputStream = getClass.getResourceAsStream(path)
    if (resourceInputStream == null) {
      throw new NullPointerException("Can't find the resource in classpath: " + path)
    }
    val source = Source.fromInputStream(resourceInputStream)("UTF-8")
    val string = source.mkString
    source.close()
    string
  }
}
