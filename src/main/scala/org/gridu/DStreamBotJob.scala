package org.gridu

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.gridu.cassandra.CassandraUtils
import org.gridu.config.{AppConfig, BotConfig}
import org.gridu.spark.{Click, EnrichedClick, KafkaDStreamProvider, Spark}
import org.gridu.util.Classifier
import org.gridu.util.JsonOperations._
import org.gridu.util.TextUtils._

import scala.language.implicitConversions
import scala.util.Try

object DStreamBotJob extends App with Spark {

  val appConfig = AppConfig()

  val ssc = new StreamingContext(spark.sparkContext, appConfig.botConfig.batchDuration)
  private val stream = new KafkaDStreamProvider(ssc, appConfig.kafka).stream.map(_.value)
  private val clicks = getClicks(stream, filterMalformed)

  detectBots(
    clicks,
    CassandraUtils.writeToCassandra(_, appConfig.cassandra),
    appConfig.botConfig
  )

  ssc.checkpoint("/tmp/sparkCheckpoint")
  ssc.remember(Seconds(35))
  ssc.start()
  ssc.awaitTermination()

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

        saveFunction(enriched)
      }
      )

  implicit def toDuration(number: Long): Duration = Duration(number)

  def filterMalformed(input: RDD[Try[Click]]) = input.flatMap(_.toOption)

  def getClicks(stream: DStream[String],
                handleMalformed: RDD[Try[Click]] => RDD[Click]):
  DStream[Click] =
    stream
      .map(str => Try(removeQuotesAndEscape(str).as[Click]))
      .flatMap(_.toOption)
}


