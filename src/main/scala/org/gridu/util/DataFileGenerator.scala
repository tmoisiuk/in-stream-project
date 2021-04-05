package org.gridu.util

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.google.common.io.Files
import org.gridu.spark.Click

import scala.language.postfixOps
import scala.util.{Random, Try}

object DataFileGenerator extends App {
  val tmpFileName = "data/records.dat"
  val resultFileName = "/Users/tmoisiuk/IdeaProjects/in-stream-project/kafka-connect/clicks.txt"

  val tmpFile = new java.io.File(tmpFileName)

  tmpFile.getParentFile.mkdirs()

  val startApp = System.currentTimeMillis()

  val pw = new java.io.PrintWriter(tmpFile)

  var total = 0

  val writeResult = Try {
    val tillUnixTime = LocalDateTime.of(2020, 2, 29, 14, 20, 35, 234000).toInstant(ZoneOffset.UTC)
    val duration = Duration.ofMinutes(10)

    val start = tillUnixTime.minus(duration).getEpochSecond
    val end = tillUnixTime.getEpochSecond

    for {
      s <- start.until(end, 60)
    } {
      val i = Instant.ofEpochSecond(s)

      val bots = Seq(
        activity(i, "0.0.0.1", 10),
        activity(i, "0.0.0.2", 10),
        activity(i, "0.0.0.3", 10),
      )

      val users = for {
        a <- 1 to 10
        d <- 1 to 52
        act <- activity(i, s"$a.$a.$d.$d", 10)
      } yield act

      Random.shuffle(bots.flatten ++ users).foreach { click =>

        import JsonOperations._
        pw.println(click.toJson)
        total += 1
      }
    }
  }

  writeResult
    .flatMap(_ => Try {
      val resFile = new java.io.File(resultFileName)
      resFile.getParentFile.mkdirs()
      Files.move(tmpFile, resFile)
    })
    .recover {
      case e: Exception => e.printStackTrace()
    }
    .foreach(_ => pw.close())

  println(s"$total items were written to $resultFileName in ${System.currentTimeMillis() - startApp} ms")

  def activity(time: Instant, ip: String, num: Int) = {
    val url = "https://blog.griddynamics.com/in-stream-processing-service-blueprint"
    Seq.fill(num) {
      Click("click", ip, time.getEpochSecond, url)
    }
  }
}
