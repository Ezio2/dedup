package com.dedup.comment.data

import com.github.nscala_time.time.Imports.DateTime
import com.ibm.icu.text.Transliterator
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.hashing.MurmurHash3


case class Comment(id: Long, content: String, createTime: DateTime,
                   k: Int = Comment.k, r: Int = Comment.r, b: Int = Comment.b) {
  require(k >= 1 && r >= 1 && b >= 1, "minhash arguments illegal")

  val rawSignatures: Set[String] = content.sliding(k).toSet

  val signatures: Set[Int] = rawSignatures.map(MurmurHash3.stringHash)

  val minhashSignatures: Vector[Int] = (1 to (r * b)).par.map(i =>
    rawSignatures.map(MurmurHash3.stringHash(_, i)).min).toVector

  val recallSignatures: Set[Int] = minhashSignatures.grouped(r).zipWithIndex.map {
    case (x, i) => MurmurHash3.arrayHash(x.toArray :+ i)
  }.toSet

}

object Comment {
  private val conf = ConfigFactory.load("comment")
  private val log = Logger.getLogger(this.getClass.getSimpleName)

  implicit val formats = DefaultFormats

  private val transliterator = Transliterator.getInstance("Fullwidth-Halfwidth")
  private val k = conf.getInt("minhash.k")
  private val r = conf.getInt("minhash.row")
  private val b = conf.getInt("minhash.banding")

  def apply(message: String): Option[Comment] = {
    try {
      val json = parse(message)
      val id = (json \ "id").extract[Long]
      val content = transliterator.transliterate(
        (json \ "content").extract[String].
          replaceAll("\\pP|\\pS|\\s", " ").replaceAll("\\s+", " ").trim.toLowerCase)
      val createTime = new DateTime((json \ "create_time").extract[Long] * 1000)
      Some(Comment(id, content, createTime))
    } catch {
      case t: Throwable =>
        log.error(s"message:$message parse something wrong:" + "\n" +
          t + "\n" + t.getStackTrace.mkString("\n"))
        None
    }
  }

  def apply(id: Long, rawContent: String, createSeconds: Long): Option[Comment] = {
    val content = transliterator.transliterate(
      rawContent.replaceAll("\\pP|\\pS|\\s", " ").replaceAll("\\s+", " ").trim.toLowerCase)
    val createTime = new DateTime(createSeconds * 1000)
    Some(Comment(id, content, createTime))
  }
}
