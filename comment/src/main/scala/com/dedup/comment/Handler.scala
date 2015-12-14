package com.dedup.comment

import com.dedup.comment.data.Article.ArticleDeserializable
import com.dedup.comment.data.{Article, Comment, Result}
import com.dedup.scindex.Conversions._
import com.dedup.scindex.{ForwardIndex, InvertIndex}
import com.dedup.util.{Distance, IdGenerator, TimeMeasure}
import org.apache.log4j.Logger
import com.github.nscala_time.time.Imports._

import scala.collection.mutable

class Handler(forwardIndexDir: String,
              forwardIndexCacheSize: Int,
              forwardIndexTtl: Long,
              invertIndexDir: String,
              invertIndexCacheSize: Int,
              invertIndexTtl: Long,
              idGenerator: IdGenerator,
              threshold: Double) {
  val log = Logger.getLogger(this.getClass.getSimpleName)
  private val forwardIndex = new ForwardIndex[Long, Article](forwardIndexDir, forwardIndexCacheSize, forwardIndexTtl)
  private val invertIndex = new InvertIndex[Int, Long](invertIndexDir, invertIndexCacheSize, invertIndexTtl)

  def handle(messages: List[String]): List[String] = {
    implicit val profile = mutable.Map[String, Long]()
    val comments = TimeMeasure.profile("getMessages") {
      messages.par.flatMap(Comment(_)).toVector
    }
    val existsResults = mutable.ArrayBuffer[(Comment, Article)]()
    val newComments = mutable.ArrayBuffer[Comment]()
    comments.par.map(c => c -> forwardIndex.get(c.id)).toList.foreach {
      case (c, o) => o match {
        case Some(ar) => existsResults += ((c, ar))
        case None => newComments += c
      }
    }
    val dedupResults = TimeMeasure.profile("dedup") {
      dedup(newComments.toVector)
    }
    val results = TimeMeasure.profile("merge") {
      merge(dedupResults) ++ existsResults
    }
    results.par.map(r => Result(r._1.id, r._2.clusterId).dump).toList
  }

  def dedupOne(comment: Comment, articles: List[Article]): Option[Article] = if (articles.isEmpty) None
  else {
    val cand = articles.par.map(a => a -> Distance.jaccard(a.signatures, comment.signatures)).maxBy(_._2)
    if (cand._2 >= threshold) Some(cand._1) else None
  }

  def dedupOne(comment: Comment): Option[Article] = {
    implicit val profile = mutable.Map[String, Long]()
    val articleIds = TimeMeasure.profile("invertIndex") {
      comment.recallSignatures.par.flatMap(invertIndex.get).toVector
    }
    val articles = TimeMeasure.profile("forwardIndex") {
      forwardIndex.mget(articleIds).values.flatten.toList
    }
    dedupOne(comment, articles)
  }

  def dedup(comments: Vector[Comment]): List[(Comment, Option[Article])] = comments.par.map(c => c -> dedupOne(c)).toList

  def merge(results: List[(Comment, Option[Article])]): List[(Comment, Article)] = {
    val r = mutable.Map[Comment, Article]()
    results.sortBy(_._1.createTime).foreach {
      case (c, o) =>
        val article = dedupOne(c, (r ++ o.map(x => x.id -> x).toMap).values.toList) match {
          case Some(ar) => ar
          case None => Article(c, idGenerator.get())
        }
        r += c -> article
    }
    r.values.par.foreach {
      case ar =>
        forwardIndex.set(ar.id, ar, ar.createTime.getMillis)
        ar.recallSignatures.par.foreach(invertIndex.add(_, ar.id, ar.createTime.getMillis))
    }
    r.toList
  }


  def close() = {
    forwardIndex.close()
    invertIndex.close()
  }
}
