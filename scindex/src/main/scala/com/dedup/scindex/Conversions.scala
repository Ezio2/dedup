package com.dedup.scindex

import com.google.common.primitives.Longs
import com.google.common.primitives.Ints


object Conversions {

  //case class 在这里承担了相等判定
  //long
  case class LongSerializable(v: Long) extends Serializable {
    override def dump: Array[Byte] = Longs.toByteArray(v)
  }

  implicit def LongToSerializable(x: Long): LongSerializable = LongSerializable(x)


  implicit object LongDeserializable extends Deserializable[Long] {
    override def load(bytes: Array[Byte]): Option[Long] = Option(Longs.fromByteArray(bytes))
  }
  //int
  case class IntSerializable(v: Int) extends Serializable {
    override def dump: Array[Byte] = Ints.toByteArray(v)
  }

  implicit def IntToSerializable(x: Int): IntSerializable = IntSerializable(x)


  implicit object IntDeserializable extends Deserializable[Int] {
    override def load(bytes: Array[Byte]): Option[Int] = Option(Ints.fromByteArray(bytes))
  }


  //string
  val charset = "utf-8"
  case class StringSerializable(v: String) extends Serializable {
    override def dump: Array[Byte] = v.getBytes(charset)
  }

  implicit def StringToSerializable(x: String): StringSerializable = StringSerializable(x)


  implicit object StringDeserializable extends Deserializable[String] {
    override def load(bytes: Array[Byte]): Option[String] = Option(new String(bytes, charset))
  }
}
