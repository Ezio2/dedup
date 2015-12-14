package com.dedup.scindex

trait Deserializable[A] {
  def load(bytes: Array[Byte]): Option[A]
}
