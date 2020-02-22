package org.gridu.spark

import org.apache.spark.util.AccumulatorV2

class ClickAccumulator extends AccumulatorV2[Seq[Click], Seq[Click]] {

  private var messages = Seq.empty[Click]

  override def isZero: Boolean = messages.isEmpty

  override def copy(): AccumulatorV2[Seq[Click], Seq[Click]] = {
    val newAccumulator = new ClickAccumulator()
    newAccumulator.messages = this.messages
    newAccumulator
  }

  override def reset(): Unit = {
    messages = Seq.empty
  }

  override def add(v: Seq[Click]): Unit = {
    messages = messages ++ v
  }

  override def merge(other: AccumulatorV2[Seq[Click], Seq[Click]]): Unit = {
    messages = messages ++ other.value
  }

  override def value: Seq[Click] = messages
}
