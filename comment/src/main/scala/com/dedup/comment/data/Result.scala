package com.dedup.comment.data

import org.json4s._
import org.json4s.jackson.Serialization.write

case class Result(commentId: Long, clusterId: Long) {
  implicit val formats = DefaultFormats

  def dump = write(Map("comment" -> commentId, "cluster" -> clusterId))
}


