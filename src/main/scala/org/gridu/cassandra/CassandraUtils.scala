package org.gridu.cassandra

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, SparkSession}
import org.gridu.config.CassandraConfig


object CassandraUtils {

  implicit class DatasetExtension[T](ds: Dataset[T]) {

    def writeToCassandraStructured(query: T => String)
                                  (implicit spark: SparkSession): DataStreamWriter[T] =
      ds.writeStream
        .foreach(new CassandraWriter(CassandraConnector(spark.sparkContext.getConf), query))
  }

  def writeToCassandra[A](bots: RDD[A], config: CassandraConfig)
                         (implicit m: Manifest[A]): Unit = {

    import com.datastax.spark.connector._

    bots
      .saveToCassandra(
        config.keySpace,
        config.table,
        AllColumns,
        writeConf = WriteConf(
          ttl = TTLOption.constant(600),
          ifNotExists = true
        )
      )
  }

}
