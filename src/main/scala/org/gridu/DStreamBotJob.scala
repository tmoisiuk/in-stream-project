package org.gridu

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.gridu.config.{AppConfig, BotConfig}
import org.gridu.spark.{Click, EnrichedClick, KafkaDStreamProvider, Spark}
import org.gridu.util.Classifier
import org.gridu.util.JsonOperations._


import scala.util.Try

object DStreamBotJob extends App with Spark {

  val appConfig = AppConfig()

  val ssc = new StreamingContext(spark.sparkContext, appConfig.botConfig.batchDuration)
  private val stream = new KafkaDStreamProvider(ssc, appConfig.kafka).stream
  private val clicks = getClicks(stream, filterMalformed)

  detectBots(clicks, save, appConfig.botConfig)


  def detectBots(clicks: DStream[Click],
                 saveFunction: RDD[EnrichedClick] => Unit,
                 botConfig: BotConfig): Unit =
    clicks
      .window(botConfig.windowDuration, botConfig.slideDuration)
      .foreachRDD(clicks => {

        val enriched = clicks
          .groupBy(_.ip)
          .flatMap { case (_, groupedClicks) =>
            new Classifier(botConfig.botTimeThreshold, botConfig.botMessagesNumber).classify(groupedClicks)
          }

        save(enriched)
      }
      )

  implicit def toDuration(number: Long): Duration = Duration(number)

  def save(rdd: RDD[EnrichedClick]): Unit = ???

  def filterMalformed(input: RDD[Try[Click]]) = input.flatMap(_.toOption)

  def getClicks(stream: DStream[ConsumerRecord[String, String]],
                handleMalformed: RDD[Try[Click]] => RDD[Click]):
  DStream[Click] =
    stream
      .map { consumerRecord => Try(consumerRecord.value.as[Click]) }
      .flatMap(_.toOption)
}

