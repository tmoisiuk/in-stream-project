package org.gridu.config

import org.scalatest.funsuite.AnyFunSuite

class AppConfigTest extends AnyFunSuite {

  test("AppConfig creation test") {

    val actual = AppConfig("/test.conf")

    val expected = AppConfig(
      SparkConfig("app-name", "mode"),
      KafkaConfig(
        "click-stream",
        Seq("localhost:9092"),
        "group_id1",
        "earliest",
        enableAutoCommit = false),
      BotConfig(10, 20, 5000, 60, 50),
      CassandraConfig("127.0.0.1", "7000", "key-space", "table")
    )

    assert(actual == expected)

  }
}
