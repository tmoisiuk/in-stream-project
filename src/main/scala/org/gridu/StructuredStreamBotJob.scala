package org.gridu

import org.apache.spark.sql.Dataset
import org.gridu.config.{AppConfig, BotConfig}
import org.gridu.spark.StructuredUtils._
import org.gridu.spark.{Click, EnrichedClick, KafkaStructuredStreamProvider, Spark}
import org.gridu.util.JsonOperations._

import scala.util.Try

object StructuredStreamBotJob extends App with Spark {

  val appConfig = AppConfig()

  private val streamingDataset = new KafkaStructuredStreamProvider(spark, appConfig.kafka).streamingDataset

  val clicks = filterMalformed(streamingDataset)

  detectBots(clicks, save, appConfig.botConfig)

  def detectBots(clicks: Dataset[Click],
                 saveFunction: Dataset[EnrichedClick] => Unit,
                 botConfig: BotConfig): Unit = {

    val enriched = classify(clicks, appConfig.botConfig, spark)
    saveFunction(enriched)
  }

  def save(rdd: Dataset[EnrichedClick]): Unit = ???

  def filterMalformed(input: Dataset[(String, String)]): Dataset[Click] = {
    import spark.implicits._
    input.map { case (_, value) => Try(value.as[Click]) }
      .flatMap(_.toOption)
  }
}
