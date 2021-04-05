package org.gridu.spark

case class Click(`type`: String,
                 ip: String,
                 event_time: Long,
                 url: String) {

  def enrichAsBot = EnrichedClick(`type`, ip, is_bot = true, event_time, url)

  def enrichAsNotBot = EnrichedClick(`type`, ip, is_bot = false, event_time, url)
}

case class EnrichedClick(`type`: String,
                         ip: String,
                         is_bot: Boolean,
                         event_time: Long,
                         url: String)
