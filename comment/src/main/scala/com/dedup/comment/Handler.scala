package com.dedup.comment

import com.dedup.comment.data.Article.ArticleDeserializable
import com.dedup.comment.data.{Article, Comment, Result}
import com.dedup.scindex.Conversions._
import com.dedup.scindex.{ForwardIndex, InvertIndex}
import com.dedup.util.{Similarity, IdGenerator, TimeMeasure}
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
              jt: Double, lf: Double, lt: Double) {
  val log = Logger.getLogger(this.getClass.getSimpleName)
  private val forwardIndex = new ForwardIndex[Long, Article](forwardIndexDir, forwardIndexCacheSize, forwardIndexTtl)
  private val invertIndex = new InvertIndex[Int, Long](invertIndexDir, invertIndexCacheSize, invertIndexTtl)

  def handler(comments: List[Comment]): Map[Long, Long] = {
    implicit val profile = mutable.Map[String, Long]()
    val existsResults = mutable.ArrayBuffer[(Comment, Article)]()
    val newComments = mutable.ArrayBuffer[Comment]()
    TimeMeasure.profile("new") {
      comments.par.map(c => c -> forwardIndex.get(c.id)).toList.foreach {
        case (c, o) => o match {
          case Some(ar) => existsResults += ((c, ar))
          case None => newComments += c
        }
      }
    }
    val dedupResults = TimeMeasure.profile("dedup") {
      dedup(newComments.toVector)
    }
    val results = TimeMeasure.profile("merge") {
      merge(dedupResults) ++ existsResults
    }
    val r = TimeMeasure.profile("results") {
      results.map(r => r._1.id -> r._2.clusterId).toMap
    }
    TimeMeasure.logProfile(profile, "dedup")
    r
  }

  def handle(messages: List[String]): List[String] = {
    implicit val profile = mutable.Map[String, Long]()
    val comments = TimeMeasure.profile("getMessages") {
      messages.par.flatMap(Comment(_)).toVector
    }
    val existsResults = mutable.ArrayBuffer[(Comment, Article)]()
    val newComments = mutable.ArrayBuffer[Comment]()
    TimeMeasure.profile("new") {
      comments.par.map(c => c -> forwardIndex.get(c.id)).toList.foreach {
        case (c, o) => o match {
          case Some(ar) => existsResults += ((c, ar))
          case None => newComments += c
        }
      }
    }
    val dedupResults = TimeMeasure.profile("dedup") {
      dedup(newComments.toVector)
    }
    val results = TimeMeasure.profile("merge") {
      merge(dedupResults) ++ existsResults
    }
    val r = TimeMeasure.profile("results") {
      results.par.map(r => Result(r._1.id, r._2.clusterId).dump).toList
    }
    TimeMeasure.logProfile(profile, "dedup")
    r
  }

  def dedupOne(comment: Comment, articles: List[Article]): Option[Article] = if (articles.isEmpty) None
  else {
    val cand1 = articles.par.map(a =>
      a -> Similarity.jaccard(a.signatures, comment.signatures)).filter(_._2 >= lf)
    val cand2 = cand1.par.map(x => (x._1, x._2, Similarity.levenstein(x._1.content, comment.content))).filter(
      x => x._2 >= jt || x._3 >= lt)
    if (cand2.nonEmpty) Some(cand2.maxBy(x => (x._2, x._3))._1) else None
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
      case (c, oa) =>
        val article = dedupOne(c, r.values.toList ++ oa) match {
          case Some(ar) => Article(c, ar.clusterId)
          case None => Article(c, idGenerator.get())
        }
        r += c -> article
        log.info(s"comment:${c.id} cluster to article:${article.id} cluster:${article.clusterId}")
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
