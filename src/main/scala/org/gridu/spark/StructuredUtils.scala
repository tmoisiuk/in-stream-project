package org.gridu.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gridu.config.BotConfig

object StructuredUtils {

  case class GroupedClicks(ip: String,
                           types:
                           List[String],
                           event_times:
                           List[Long],
                           urls: List[String],
                           isBot: Boolean)

  def classify(clicks: Dataset[Click], botConfig: BotConfig, spark: SparkSession): Dataset[EnrichedClick] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    clicks
      .withColumn("timestamp", to_timestamp('event_time))
      .withColumn("event_count", lit(1))
      .groupBy('ip, window(
        'timestamp,
        botConfig.botTimeThreshold + " seconds",
        "1 second"
      ))
      .agg(
        collect_list($"type").alias("types"),
        collect_list($"event_time").alias("event_times"),
        collect_list($"url").alias("urls"),
        (count('event_count) > botConfig.botMessagesNumber).alias("isBot")
      )
      .drop('window)
      .drop('event_count)
      .drop('timestamp)
      .as[GroupedClicks]
      .flatMap(group =>
        (group.types zip group.event_times zip group.urls).map {
          case ((typ, event_time), url) => EnrichedClick(group.ip, typ, group.isBot, event_time, url)
        }
      )
  }
}
