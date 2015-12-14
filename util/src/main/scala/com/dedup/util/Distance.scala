package com.dedup.util

object Distance {
  def jaccard[A](set1: Set[A], set2: Set[A]): Double = if (set1.size + set2.size > 0) (set1 & set2).size.toDouble / (set1 | set2).size else 0
}
