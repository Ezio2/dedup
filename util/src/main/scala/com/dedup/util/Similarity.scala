package com.dedup.util

object Similarity {
  def jaccard[A](set1: Set[A], set2: Set[A]): Double = if (set1.size + set2.size > 0) (set1 & set2).size.toDouble / (set1 | set2).size else 0
  def jaccardMax[A](set1: Set[A], set2: Set[A]): Double = if (set1.size + set2.size > 0) (set1 & set2).size.toDouble / List(set1.size, set2.size).max else 0

  def levenstein(str1: String, str2: String) = if (str1.nonEmpty || str2.nonEmpty) 1 - _levenshtein(str1, str2).toDouble / List(str1.length, str2.length).max else 0.0

  private def _levenshtein(str1: String, str2: String): Int = {
    val lenStr1 = str1.length
    val lenStr2 = str2.length

    val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)

    for (i <- 0 to lenStr1) d(i)(0) = i
    for (j <- 0 to lenStr2) d(0)(j) = j

    for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
      val cost = if (str1(i - 1) == str2(j-1)) 0 else 1

      d(i)(j) = min(
        d(i-1)(j  ) + 1,     // deletion
        d(i  )(j-1) + 1,     // insertion
        d(i-1)(j-1) + cost   // substitution
      )
    }

    d(lenStr1)(lenStr2)
  }

  private def min(nums: Int*): Int = nums.min
}
