package com.dedup.util

import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.duration._


object Done {
  private val log = Logger.getLogger(this.getClass.getSimpleName)

  @tailrec
  def retry[A](body: => A, delay: FiniteDuration = 2.seconds, backoff: Int = 1): A = try { body } catch {
    case t: Throwable =>
      log.error(s"promiseDone something wrong:" + "\n" +
        t + "\n" + t.getStackTrace.mkString("\n"))
      Thread.sleep(delay.toMillis)
      retry(body, delay * backoff, backoff)
  }

  def neverStop(body: => Unit, sleepTime: FiniteDuration = 2.seconds): Unit = {
    while (true) {
      try {
        body
      } catch {
        case t: Throwable =>
          log.error(s"neverDone something wrong:" + "\n" +
            t + "\n" + t.getStackTrace.mkString("\n"))
          Thread.sleep(sleepTime.toMillis)
      }
    }
  }

  def promiseTrue(body: => Boolean, sleepTime: FiniteDuration = 2.seconds): Unit = {
    var done = false
    while (!done) {
      try {
        done = body
      } catch {
        case t: Throwable =>
          log.error(s"neverDone something wrong:" + "\n" +
            t + "\n" + t.getStackTrace.mkString("\n"))
          Thread.sleep(sleepTime.toMillis)
      }
    }
  }

}
