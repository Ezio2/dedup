package com.dedup.util

import org.apache.log4j.Logger

import scala.collection.mutable


object TimeMeasure {
  val log = Logger.getLogger(this.getClass.getSimpleName)

  def timeMeasure[A](block: => A): (Long, A) = {
    val start = System.currentTimeMillis()
    val r = block
    val end = System.currentTimeMillis()
    (end - start, r)
  }

  def profile[A](key: String)(block: => A)(implicit profiler: mutable.Map[String, Long]): A = {
    val start = System.currentTimeMillis()
    val r = block
    val end = System.currentTimeMillis()
    profiler(key) = profiler.getOrElse(key, 0L) + (end - start)
    r
  }

  def logProfile(profiler: mutable.Map[String, Long], name: String, debug: Boolean = false): Unit = {
    val total = profiler.values.sum
    profiler.foreach {
      case (pname, delay) =>
        if (debug)
          log.debug(s"[$name-profile] name: $pname, delay: $delay ms, ratio: ${delay.toDouble / total}")
        else
          log.info(s"[$name-profile] name: $pname, delay: $delay ms, ratio: ${delay.toDouble / total}")
    }
    if (debug)
      log.debug(s"[$name-profile] total cost $total ms")
    else
      log.info(s"[$name-profile] total cost $total ms")
  }

}
