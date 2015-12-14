package com.dedup.scindex.cache

import com.twitter.util.SynchronizedLruMap

class ForwardCache[A, B](cacheSize: Int) {
  private val cache = new SynchronizedLruMap[A, B](cacheSize)

  def get(key: A): Option[B] = cache.get(key)

  def put(key: A, value: B): Unit = cache.put(key, value)

  def putIfKeyPresent(key: A, value: B): Unit = if (cache.contains(key)) cache.put(key, value)

  def remove(key: A): Unit = cache.remove(key)

  def size = cache.size
}
