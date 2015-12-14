package com.dedup.scindex

import java.nio.file.Files

import com.dedup.util.TimeMeasure
import org.scalatest.FunSuite
import Conversions._
import scala.sys.process._


class InvertIndexTest extends FunSuite {
  val tempDir = Files.createTempDirectory("invertIndex")
  val index = new InvertIndex[String, Long](tempDir.toString, 1000, 1000 * 3600)
  test("basic operation"){
    val num = 1000L
    val data = (1L to num).toVector
    val time = System.currentTimeMillis
    val (setT, _) = TimeMeasure.timeMeasure {
      data.foreach(k => data.foreach(index.add(k.toString, _, time)))
    }
    info(s"add ${num * num} items cost $setT millis")

    val (getT, _) = TimeMeasure.timeMeasure {
      data.map(_.toString).foreach(x => assert(index.get(x) == data))
    }
    info(s"get ${num * num} items cost $getT millis")

    Seq("rm", "-rf", tempDir.toString).!!

  }

  test("ttl") {
    val tempDir1 = Files.createTempDirectory("forwardIndex")
    val index1 = new InvertIndex[Long, String](tempDir1.toString, 1000, 1000)

    index1.add(1L, "test", 0)
    Thread.sleep(7000)
    assert(index1.get(1L).isEmpty)

    Seq("rm", "-rf", tempDir1.toString).!!
  }
}
