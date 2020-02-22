package org.gridu.cassandra

import org.apache.spark.rdd.RDD
import org.gridu.config.CassandraConfig
import org.gridu.spark.{EnrichedClick, TestSpark}
import org.scalatest.funsuite.AnyFunSuite

class CassandraUtilsTest extends AnyFunSuite with TestSpark {

  ignore("write to Cassandra") {

    val config = CassandraConfig("0.0.0.0", "9042", "botdetection", "enrichedclicks")

    val rdd: RDD[EnrichedClick] = spark.sparkContext.parallelize(
      Seq(
        EnrichedClick("click", "10.186.10.10", is_bot = true, 123123, "http://"),
        EnrichedClick("click", "10.186.10.11", is_bot = true, 123123, "http://")
      )
    )

    CassandraUtils.writeToCassandra(rdd, config)
  }
}
