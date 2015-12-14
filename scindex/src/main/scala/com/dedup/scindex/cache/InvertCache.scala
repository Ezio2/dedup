package com.dedup.scindex.cache

import com.google.common.collect.Sets
import com.twitter.util.SynchronizedLruMap

import scala.collection.JavaConversions._


class InvertCache[A, B](cacheSize: Int) {

  private val cache = new SynchronizedLruMap[A, java.util.Set[B]](cacheSize)

  def get(key: A): Option[Vector[B]] = cache.get(key) match {
    case Some(values) => Some(values.toVector)
    case None => None
  }

  def putIfKeyPresent(key: A, value: B): Unit = cache.get(key).foreach(_.add(value))

  def put(key: A, values: Vector[B]): Unit = cache.put(key, Sets.newConcurrentHashSet[B](values))

  def remove(key: A, value: B): Unit = cache.get(key).par.foreach(_.remove(value))

  def size = cache.size
}
