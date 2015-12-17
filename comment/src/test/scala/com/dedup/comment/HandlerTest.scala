package com.dedup.comment

import com.dedup.comment.data.{Article, Comment}
import org.apache.log4j.{Logger, Level, PatternLayout, ConsoleAppender}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import com.dedup.util.{Similarity, IdGenerator}
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
  val threshold = 0.6
  val handler = new Handler(forwardDir.toString, 50000, 3600 * 1000,
    invertDir.toString, 50000, 3600 * 1000, idGenerator, threshold, threshold)

  val data = List(
    List(
      Map("id" -> 1, "content"-> "看我名子一个月让你瘦15斤，拖", "create_time" -> System.currentTimeMillis),
      Map("id" -> 2, "content"-> "看我名子一个月让你瘦15斤，损", "create_time" -> System.currentTimeMillis),
      Map("id" -> 3, "content"-> "看我名子一个月让你瘦15斤，瘸", "create_time" -> System.currentTimeMillis),
      Map("id" -> 4, "content"-> "看我名子一个月让你瘦15斤，制", "create_time" -> System.currentTimeMillis),
      Map("id" -> 5, "content"-> "看我名子一个月让你瘦15斤，谭", "create_time" -> System.currentTimeMillis)
    ),

    List(
      Map("id" -> 6, "content"-> "总算找到牛逼靠谱的人了，荣自林 他入市十来年有丰富实战经验的。推荐给小散的，真是打心眼里敬佩他，这个从来没有求过什么回报的", "create_time" -> System.currentTimeMillis),
      Map("id" -> 7, "content"-> "总算找到牛逼靠谱的人了，荣自林 他入市十来年有丰富实战经验的。推荐给小散的，真是打心眼里敬佩他，他都没有求过什么回报的", "create_time" -> System.currentTimeMillis)
    ),

    List(
      Map("id" -> 8, "content"-> "瘦身加jjjFFF567沥墓60rT", "create_time" -> System.currentTimeMillis),
      Map("id" -> 9, "content"-> "瘦身加jjjFFF567针履47iT", "create_time" -> System.currentTimeMillis),
      Map("id" -> 10, "content"-> "瘦身加jjjFFF567赂祁04UF", "create_time" -> System.currentTimeMillis),
      Map("id" -> 11, "content"-> "瘦身加jjjFFF567悦柳82Cm", "create_time" -> System.currentTimeMillis)

    ),

    List(
      Map("id" -> 12, "content"-> "只是想简单的參考下，不过不得不佩服。百度' 水语婷 那里对个股总结的确不错啊asd", "create_time" -> System.currentTimeMillis),
      Map("id" -> 13, "content"-> "只是想简单的參考下，不过不得不佩服。百度' 水语婷那里对个股总结的确不错啊 SADASD", "create_time" -> System.currentTimeMillis)
    ),

    List(
      Map("id" -> 14, "content"-> "各位胖子们夷夷可以看我头象", "create_time" -> System.currentTimeMillis),
      Map("id" -> 15, "content"-> "各位胖子们可以看我头象", "create_time" -> System.currentTimeMillis)
    ),

    List(
      Map("id" -> 16, "content"-> "有便秘的朋友倘看我名字倘", "create_time" -> System.currentTimeMillis),
      Map("id" -> 17, "content"-> "有便秘的朋友唯看我名字唯", "create_time" -> System.currentTimeMillis),
      Map("id" -> 18, "content"-> "有便秘的朋友勿看我名字勿", "create_time" -> System.currentTimeMillis),
      Map("id" -> 19, "content"-> "有便秘的朋友人看我名字人", "create_time" -> System.currentTimeMillis),
      Map("id" -> 20, "content"-> "有便秘的朋友榆看我名字榆", "create_time" -> System.currentTimeMillis)
    )
  )

  val dataStr = data.flatMap(x => x.map(write(_)))

  def distance(a: String, b: String) = {
    val ca = Article(Comment(a).get, 0L)
    val cb = Article(Comment(b).get, 1L)
    val jSimilarity = Similarity.jaccard(ca.signatures, cb.signatures)
    val lSimilarity = Similarity.levenstein(ca.content, cb.content)
    if (jSimilarity > threshold / 2) info(s"jSimilar:$jSimilarity lSimilar:$lSimilarity id:${ca.id} vs id:${cb.id} ")
  }

  test("data similarity") {
    dataStr.combinations(2).foreach(x => distance(x.head, x(1)))
  }

  test("correctness") {
    val results = dataStr.grouped(3).flatMap(handler.handle).toList
    val r1 = results.map(
      read[Map[String, Long]]).groupBy(_("cluster")).mapValues(_.map(_("comment")).distinct)
    assert(r1.values.map(_.sorted).toSet == data.map(x => x.map(_("id"))).toSet)
    assert(results.toSet == handler.handle(dataStr).toSet)

  }
  Seq("rm", "-rf", forwardDir.toString).!!
  Seq("rm", "-rf", invertDir.toString).!!
}
