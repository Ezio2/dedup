package com.dedup.comment

import com.dedup.comment.data.Comment
import org.apache.log4j.{Logger, Level, PatternLayout, ConsoleAppender}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import com.dedup.util.{Distance, IdGenerator}
import org.scalatest.FunSuite
import java.nio.file.Files
import scala.sys.process._


class HandlerTest extends FunSuite {
  implicit val formats = DefaultFormats

  val console = new ConsoleAppender(); //create appender
  //configure the appender
  console.setLayout(new PatternLayout("%d [%p|%c|%C{1}] %m%n"))
  console.setThreshold(Level.INFO)
  console.activateOptions()
  Logger.getRootLogger.addAppender(console)

  val forwardDir = Files.createTempDirectory("forwardIndex")
  val invertDir = Files.createTempDirectory("invertIndex")
  val idGenerator = new IdGenerator("", true)

  val handler = new Handler(forwardDir.toString, 50000, 3600 * 1000, invertDir.toString, 50000, 3600 * 1000, idGenerator, 0.5)

  val data1 = List(
    Map("id" -> 1, "content"-> "看我名子一个月让你瘦15斤，拖", "create_time" -> System.currentTimeMillis),
    Map("id" -> 2, "content"-> "看我名子一个月让你瘦15斤，损", "create_time" -> System.currentTimeMillis),
    Map("id" -> 3, "content"-> "看我名子一个月让你瘦15斤，瘸", "create_time" -> System.currentTimeMillis),
    Map("id" -> 4, "content"-> "看我名子一个月让你瘦15斤，制", "create_time" -> System.currentTimeMillis),
    Map("id" -> 5, "content"-> "看我名子一个月让你瘦15斤，谭", "create_time" -> System.currentTimeMillis)
  )


  val data2 = List(
    Map("id" -> 6, "content"-> "总算找到牛逼靠谱的人了，荣自林 他入市十来年有丰富实战经验的。推荐给小散的，真是打心眼里敬佩他，这个从来没有求过什么回报的", "create_time" -> System.currentTimeMillis),
    Map("id" -> 7, "content"-> "总算找到牛逼靠谱的人了，荣自林 他入市十来年有丰富实战经验的。推荐给小散的，真是打心眼里敬佩他，他都没有求过什么回报的", "create_time" -> System.currentTimeMillis)
  )


  val data3 = List(
    Map("id" -> 8, "content"-> "瘦身加jjjFFF567沥墓60rT", "create_time" -> System.currentTimeMillis),
    Map("id" -> 9, "content"-> "瘦身加jjjFFF567针履47iT", "create_time" -> System.currentTimeMillis),
    Map("id" -> 10, "content"-> "瘦身加jjjFFF567赂祁04UF", "create_time" -> System.currentTimeMillis),
    Map("id" -> 11, "content"-> "瘦身加jjjFFF567悦柳82Cm", "create_time" -> System.currentTimeMillis)

  )

  val data4 = List(
    Map("id" -> 12, "content"-> "只是想简单的參考下，不过不得不佩服。百度' 水语婷 那里对个股总结的确不错啊asd", "create_time" -> System.currentTimeMillis),
    Map("id" -> 13, "content"-> "只是想简单的參考下，不过不得不佩服。百度' 水语婷那里对个股总结的确不错啊 SADASD", "create_time" -> System.currentTimeMillis)
  )

  val data5 = List(
    Map("id" -> 14, "content"-> "各位胖子们夷夷可以看我头象", "create_time" -> System.currentTimeMillis),
    Map("id" -> 15, "content"-> "各位胖子们可以看我头象", "create_time" -> System.currentTimeMillis)
  )

  val data = (data1 ++ data2 ++ data3 ++ data4 ++ data5).map(write(_))

  def distance(a: String, b: String): Unit = {
    val ca = Comment(a).get
    val cb = Comment(b).get
    val similarity = Distance.jaccard(ca.signatures, cb.signatures)
    if (similarity > 0) info(s"similar:$similarity id:${ca.id} content:${ca.content} vs id:${cb.id} content:${cb.content}")
  }

  test("data similarity") {
    data.combinations(2).foreach(x => distance(x.head, x(1)))
  }

  test("correct") {
    info(s"${forwardDir.toString}")
    val results = data.grouped(7).flatMap(handler.handle).map(read[Map[String, Long]])
    info(s"${results.toList}")
  }

  Seq("rm", "-rf", forwardDir.toString).!!
  Seq("rm", "-rf", invertDir.toString).!!
}
