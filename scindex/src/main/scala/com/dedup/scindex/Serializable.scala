package com.dedup.scindex

trait Serializable {
  def dump: Array[Byte]
}
