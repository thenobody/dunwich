package net.thenobody.dunwich

import scala.util.hashing.MurmurHash3

/**
  * Created by antonvanco on 22/04/2016.
  */
object InExMain {

  def charSet(from: Int, to: Int): Set[Int] = (from to to).map { i => MurmurHash3.stringHash(i+64.toChar.toString) }.toSet

  def fact(n: Int): Int = {
    def go(nn: Int, acc: Int): Int = if (nn == 0) acc
    else go(nn - 1, acc * nn)
    go(n, 1)
  }

  def binomial(n: Int, k: Int): Int = fact(n)/(fact(n - k) * fact(k))

  def select(n: Int, sets: Set[Set[Int]]): List[Set[Int]] = {
    if (n == 1) sets.toList
    else sets.flatMap { set =>
      val remainder = sets - set
      select(n - 1, remainder).map(_ union set)
    }.toList
  }

  def selectIds(n: Int, ids: Set[Int]): Set[Set[Int]] = {
    if (n == 1) ids.map(Set(_))
    else ids.flatMap { id =>
      selectIds(n - 1, ids - id).map(_ union Set(id))
    }
  }

  def cardinality(sets: Seq[Set[Int]]): Int = (1 to sets.size).flatMap { i =>
    val coeff = math.pow(-1, i - 1)
    selectIds(i, sets.indices.toSet).toList.map { ids =>
      coeff * ids.map(sets(_)).reduce(_ union _).size
    }
  }.sum.toInt

  def main(args: Array[String]): Unit = {
    val sets = Set(charSet(1, 8), charSet(2, 10), charSet(3, 11), charSet(4, 12))
    select(3, sets)
  }

}
