package org.gridu

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Duration
import org.gridu.config.BotConfig
import org.gridu.spark.{Click, StructuredUtils}
import org.scalatest.funsuite.AnyFunSuite

class StructuredStreamBotJobTest extends AnyFunSuite {


  test("structure stream test") {

    lazy val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("test app")
      .getOrCreate()


    import spark.implicits._
    val value = Seq(Click("click", "10.124.434.34", 123123, "http://")).toDS()
    val config = BotConfig(2, 10, 2, 10, 10)

    val value2 = StructuredUtils.classify(value, config, spark)

    value2.show()

  }

  //todo сделать синк часть. Смотреть редис


}
