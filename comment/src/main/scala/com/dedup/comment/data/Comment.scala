package com.dedup.comment.data

import com.github.nscala_time.time.Imports.DateTime
import com.ibm.icu.text.Transliterator
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.hashing.MurmurHash3


case class Comment(id: Long, content: String, createTime: DateTime,
                   k: Int, r: Int, b: Int) {
  require(k >= 1 && r >= 1 && b >= 1, "minhash arguments illegal")

  private val transliterator = Transliterator.getInstance("Fullwidth-Halfwidth")

  val rawSignatures: Set[String] = transliterator.transliterate(
    content.replaceAll("\\pP|\\pS|\\s", " ").replaceAll("\\s+", " ").trim
  ).sliding(k).toSet

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

  def apply(message: String): Option[Comment] = {
    try {
      val json = parse(message)
      if ((json \ "version").extractOrElse[String]("0.0") == "1.0") {
        val id = (json \ "id").extract[Long]
        val content = (json \ "content").extract[String]
        val createTime = new DateTime((json \ "create_time").extract[Long] * 1000)
        val k = conf.getInt("minhash.k")
        val r = conf.getInt("minhash.row")
        val b = conf.getInt("minhash.banding")
        Some(Comment(id, content, createTime, k, r, b))
      }
      else {
        log.warn(s"message:$message version is not right")
        None
      }
    } catch {
      case t: Throwable =>
        log.error(s"message:$message parse something wrong:" + "\n" +
          t + "\n" + t.getStackTrace.mkString("\n"))
        None
    }
  }
}
