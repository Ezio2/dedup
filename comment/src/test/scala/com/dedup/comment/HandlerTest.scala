package com.dedup.comment

import org.scalatest.FunSuite
import java.nio.file.Files
import scala.sys.process._


class HandlerTest extends FunSuite {
  val forwardDir = Files.createTempDirectory("forwardIndex")
  val invertDir = Files.createTempDirectory("invertIndex")





  Seq("rm", "-rf", forwardDir.toString).!!
  Seq("rm", "-rf", invertDir.toString).!!
}
