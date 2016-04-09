package net.thenobody.dunwich

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Created by antonvanco on 27/03/2016.
 */
package object model {

  type SketchHashes = mutable.SortedSet[Float]

  case class UserEvent(uuid: String, timestamp: Long, aspect1: Int, aspect2: Int, aspect3: Int, aspect4: Int)
  object UserEvent {
    def apply(
      uuid: String,
      timestamp: String,
      aspect1: String,
      aspect2: String,
      aspect3: String,
      aspect4: String
    ): UserEvent = new UserEvent(uuid, timestamp.toLong, aspect1.toInt, aspect2.toInt, aspect3.toInt, aspect4.toInt)
  }

  trait Aspect {
    def attribute: Int
  }
  case class Aspect1(attribute: Int) extends Aspect
  case class Aspect2(attribute: Int) extends Aspect
  case class Aspect3(attribute: Int) extends Aspect
  case class Aspect4(attribute: Int) extends Aspect

  trait Query
  case class AspectQuery(aspect: Aspect) extends Query
  case class AndQuery(terms: Seq[Query]) extends Query
  case class OrQuery(terms: Seq[Query]) extends Query

  class Sketch(val theta: Float, hashes: SketchHashes) {
    val _hashes = mutable.SortedSet(hashes.toList: _*)

    def addUser(uuid: String): Unit = {
      val hash = getHash(uuid)
      if (hash < theta) {
        _hashes.add(hash)
        if (_hashes.size > Sketch.K) {
          _hashes -= _hashes.max
        }
      }
    }

    def getHash(uuid: String): Float = MurmurHash3.stringHash(uuid) match {
      case h if h < 0 => 0.5F - 0.5F * (h / Int.MinValue.toFloat)
      case h => 0.5F + 0.5F * (h / Int.MaxValue.toFloat)
    }

    def cardinality: Int = math.round(_hashes.size / theta)

    def union(sketch: Sketch): Sketch = setOp(sketch) (_ union _)

    def intersect(sketch: Sketch): Sketch = setOp(sketch) (_ intersect _)

    def setOp(sketch: Sketch)(op: (SketchHashes, SketchHashes) => SketchHashes): Sketch = {
      val newTheta = math.min(theta, sketch.theta)
      val newHashes = op(_hashes, sketch._hashes).until(newTheta)
      new Sketch(newTheta, newHashes)
    }
  }
  object Sketch {
    val MaxHashValue = 1f
    val K = 16384
    def apply(hashes: SketchHashes): Sketch = new Sketch(hashes.max, hashes)
  }
  object EmptySketch {
    def apply(): Sketch = new Sketch(Sketch.MaxHashValue, mutable.SortedSet[Float]())
  }
}
