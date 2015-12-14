package com.dedup.util

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.ActorSystem
import org.apache.log4j._

import scala.concurrent.duration._
import scalaj.http._

class IdGenerator(url: String, local: Boolean = false, cacheSize: Int = 10000, interval: FiniteDuration = 30.milliseconds, sleepTime: FiniteDuration = 5.seconds) {
  val log = Logger.getLogger(this.getClass.getSimpleName)
  val queue = new LinkedBlockingQueue[Long](cacheSize)

  private val system = ActorSystem(this.getClass.getSimpleName)
  import system.dispatcher

  var localCount = 0

  system.scheduler.schedule(Duration.Zero, interval) {
    if (local) {
      val step = 100000
      (localCount until localCount + step).foreach(x => queue.put(x.toLong))
      localCount += step
    } else {
      try {
        val response: HttpResponse[String] = Http(url).asString
        if (response.code == 200) response.body.split(",").foreach(x => queue.put(x.toLong)) else throw new RuntimeException(s"invalid status code ${response.body.toLong}")
      }
      catch {
        case t: Throwable =>
          log.error(s"IdGenerator something wrong:" + "\n" +
            t + "\n" + t.getStackTrace.mkString("\n"))
          Thread.sleep(sleepTime.toMillis)
      }
    }
  }


  def get(): Long = queue.take()

  def close() = system.terminate()
}
