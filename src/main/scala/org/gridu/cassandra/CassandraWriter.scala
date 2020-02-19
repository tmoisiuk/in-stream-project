package org.gridu.cassandra

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter


class CassandraWriter[T](val connector: CassandraConnector,
                         query: T => String) extends ForeachWriter[T] {

  val KEYSPACE = "StreamingDB"
  val TABLE = "BotsStructured"

  def open(partitionId: Long, version: Long): Boolean = true

  def process(record: T): Unit =
    connector.withSessionDo(session => session.execute(query(record)))


  def close(errorOrNull: Throwable): Unit = {}
}

