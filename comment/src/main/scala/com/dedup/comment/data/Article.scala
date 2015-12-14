package com.dedup.comment.data


import com.dedup.scindex.{Deserializable, Serializable}
import com.github.nscala_time.time.Imports.DateTime
import org.apache.log4j.Logger
import org.msgpack.ScalaMessagePack
import org.msgpack.annotation.Message


@Message
private class ArticleMessagePack {
  var id: Long = 0L
  var clusterId: Long = 0L
  var signatures: Set[Int] = Set()
  var recallSignatures: Set[Int] = Set()
  var createTime: Long = 0L
}


case class Article(id: Long,
                   clusterId: Long,
                   signatures: Set[Int],
                   recallSignatures: Set[Int],
                   createTime: DateTime) extends Serializable {

  def dump: Array[Byte] = {
    val obj = new ArticleMessagePack
    obj.id = id
    obj.clusterId = clusterId
    obj.signatures = signatures
    obj.recallSignatures = recallSignatures
    obj.createTime = createTime.getMillis
    ScalaMessagePack.write(obj)
  }

}


object Article {
  val log = Logger.getLogger(this.getClass.getSimpleName)

  def apply(comment: Comment, clusterId: Long): Article =
    Article(comment.id, clusterId, comment.signatures, comment.recallSignatures, comment.createTime)

  implicit object ArticleDeserializable extends Deserializable[Article] {
    override def load(bytes: Array[Byte]): Option[Article] = try {
      val obj = ScalaMessagePack.read[ArticleMessagePack](bytes)
      Some(Article(
        obj.id,
        obj.clusterId,
        obj.signatures,
        obj.recallSignatures,
        new DateTime(obj.createTime)
      ))
    }
    catch {
      case t: Throwable =>
        log.warn(s"Article message pack load wrong:" + "\n" + t + "\n" + t.getStackTrace.mkString("\n"))
        None
    }
  }

}
