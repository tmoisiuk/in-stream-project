package org.gridu.util

import org.gridu.spark.{Click, EnrichedClick}

import scala.collection.mutable

class Classifier(botTimeThreshold: Long,
                 botMessagesNumber: Int) {

  def classify(clicks: Iterable[Click]): Iterable[EnrichedClick] = {
    val size = clicks.size

    if (size < botMessagesNumber) {
      clicks.map(_.enrichAsNotBot)
    }
    else {
      val botIndices = mutable.Set[Int]()
      val sorted = clicks.toList.sortBy(_.event_time)

      var j = 0
      for (i <- sorted.indices) {
        while (
          (sorted(j).event_time - sorted(i).event_time < botTimeThreshold) & (j < size - 1)) {
          j = j + 1
        }
        if (j - i > botMessagesNumber) {
          botIndices ++= Range.inclusive(i, j).toSet
        }
      }

      sorted.zipWithIndex.map {
        case (click, index) if botIndices.contains(index) => click.enrichAsBot
        case (click, _) => click.enrichAsNotBot
      }
    }
  }
}
