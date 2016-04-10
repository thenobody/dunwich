package net.thenobody.dunwich

import scala.collection.{GenTraversableOnce, mutable}
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

    def apply(parts: List[String]): UserEvent = parts match {
      case uuid +: timestamp +: aspect1 +: aspect2 +: aspect3 +: aspect4 +: Nil =>
        UserEvent(uuid, timestamp, aspect1, aspect2, aspect3, aspect4)
    }
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

  class BoundedSketch(val k: Int, val hashes: SketchHashes) {
    val membership = mutable.HashSet(hashes.toList: _*)

    import Sketch._

    def this(k: Int) = this(k, mutable.TreeSet[Float]())

    def theta: Float = if (hashes.isEmpty) 1.0F
      else hashes.max

    var _theta: Float = theta
    def addUser(uuid: String): Unit = {
      val hash = getHash(uuid)
      if (hashes.size < k) hashes += hash
      else if (hash < _theta && !membership.contains(hash)) {
        hashes += hash
        membership += hash
        val (newTheta, oldTheta) = hashes.takeRight(2).toList match { case nt +: ot+: Nil => (nt, ot)}
        hashes -= oldTheta
        membership -= oldTheta
        _theta = newTheta
      }
    }

    def cardinality: Int = math.round(hashes.size / theta)

    def mean: Double = average(hashes.map(_.toDouble))

    def variance: Double = {
      val meanValue = mean
      average(hashes.map { hash => math.pow(meanValue - hash, 2) })
    }

    def union(sketch: BoundedSketch): BoundedSketch = setOp(sketch) (_ union _)

    def intersect(sketch: BoundedSketch): BoundedSketch = setOp(sketch) (_ intersect _)

    def setOp(sketch: BoundedSketch)(op: (SketchHashes, SketchHashes) => SketchHashes): BoundedSketch = {
      val newTheta = math.min(theta, sketch.theta)
      val newHashes = op(hashes, sketch.hashes).until(newTheta)
      new BoundedSketch(k, newHashes)
    }
  }

  case class Sketch(hashes: SketchHashes) {
    import Sketch._
    val _hashes = mutable.SortedSet(hashes.toList: _*)

    def addUser(uuid: String): Unit = {
      val hash = getHash(uuid)
      _hashes.add(hash)
    }

    def cardinality(accuracy: Float): Int = {
      val sampleHashes = sample(accuracy)
      math.round(sampleHashes.size / sampleHashes.max)
    }

    private def sample(accuracy: Float): SketchHashes = _hashes.take(k(accuracy))

    def sampled(accuracy: Float): Sketch = copy(hashes = sample(accuracy))

    def union(sketch: Sketch): Sketch = setOp(sketch) (_ union _)

    def intersect(sketch: Sketch): Sketch = setOp(sketch) (_ intersect _)

    def setOp(sketch: Sketch)(op: (SketchHashes, SketchHashes) => SketchHashes): Sketch = {
      val newTheta = math.min(_hashes.max, sketch._hashes.max)
      val newHashes = op(_hashes, sketch._hashes).until(newTheta)
      new Sketch(newHashes)
    }
  }

  object Sketch {
    val MaxHashValue = 1f

    def getHash(uuid: String): Float = MurmurHash3.stringHash(uuid) match {
      case h if h < 0 => 0.5F - 0.5F * (h / Int.MinValue.toFloat)
      case h => 0.5F + 0.5F * (h / Int.MaxValue.toFloat)
    }

    def k(accuracy: Float): Int = math.round(1 / math.pow(1.0 - accuracy, 2) + 2).toInt

    val average = PartialFunction {
      iterable: GenTraversableOnce[Double] =>
        iterable.foldLeft((0, 0.0)) { case ((count, sum), hash) => (count + 1, hash + sum) }
    } andThen { case (count, sum) => sum / count }
  }

  object EmptySketch {
    def apply(): Sketch = new Sketch(mutable.SortedSet[Float]())
  }

  object BoundedSketch {
    val DefaultAccuracy = 0.95F

    def apply(): BoundedSketch = BoundedSketch(DefaultAccuracy)
    def apply(accuracy: Float): BoundedSketch = new BoundedSketch(Sketch.k(accuracy))
  }
}
