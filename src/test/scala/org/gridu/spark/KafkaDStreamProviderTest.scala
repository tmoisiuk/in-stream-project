package org.gridu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.gridu.DStreamBotJob._
import org.gridu.config.KafkaConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable

class KafkaDStreamProviderTest extends AnyFunSuite with TestSpark with Matchers {

  ignore("kafka-connect reading test") {
    val ssc = new StreamingContext(spark.sparkContext, Duration(1000))

    val conf = KafkaConfig(
      "click-stream1",
      Seq("localhost:9092"),
      "group-id1",
      "latest",
      enableAutoCommit = false
    )

    val stream = new KafkaDStreamProvider(ssc, conf)
      .stream
      .map(_.value())

    getClicks(stream, filterMalformed)
      .foreachRDD(rdd => rdd.collect().foreach(println(_)))

    ssc.start()
    ssc.awaitTermination()
  }

  test("Application should handle incoming events") {

    val clickString1 = "\"{\"type\":\"click\",\"ip\":\"10.124.434.34\",\"event_time\":123123,\"url\":\"http://\"}\""
    val clickString2 = "\"{\"type\":\"click\",\"ip\":\"10.124.434.14\",\"event_time\":12312523,\"url\":\"http://aasd\"}\""

    val click1 = Click("click", "10.124.434.34", 123123, "http://")
    val click2 = Click("click", "10.124.434.14", 12312523, "http://aasd")

    val input = Seq(
      clickString1,
      clickString2,
      "invalid string"
    )

    val streamingContext = new StreamingContext(spark.sparkContext, Duration(1000))
    val queue = mutable.Queue.empty[RDD[String]]

    val messageAccumulator = new ClickAccumulator
    spark.sparkContext.register(messageAccumulator)
    val stream = streamingContext.queueStream(queue)

    val clickStream = getClicks(stream, filterMalformed)
    clickStream.foreachRDD(rdd => messageAccumulator add rdd.collect)

    stream
      .foreachRDD(rdd => rdd.collect().foreach(d => println(d)))

    streamingContext.start()
    queue.enqueue(spark.sparkContext.parallelize(input))
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)

    val actualRows = messageAccumulator.value

    assert(actualRows.length == 2)
    actualRows must contain theSameElementsAs Seq(click1, click2)
  }
}

