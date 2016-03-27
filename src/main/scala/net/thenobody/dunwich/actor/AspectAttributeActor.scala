package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor}

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeActor extends Actor {
  val userSet: mutable.TreeSet[Float] = mutable.TreeSet()
  override def receive = {
    case UserEvent(uuid, _, _, _, _, _) =>
      addUser(uuid)

    case CardinalityRequest(accuracy) =>
      val cardinality = estimateCardinality(accuracy)
      sender() ! CardinalityResponse(cardinality.toInt, accuracy)
  }

  def addUser(uuid: String): Unit = {
    val hash: Float = MurmurHash3.stringHash(uuid) match {
      case h if h < 0 => 0.5F - 0.5F * (h / Int.MinValue.toFloat)
      case h => 0.5F + 0.5F * (h / Int.MaxValue.toFloat)
    }

    userSet += hash
  }

  def getKByAccuracy(accuracy: Float): Int = math.round(1 / math.pow(1.0 - accuracy, 2) + 2).toInt

  def estimateCardinality(accuracy: Float): Float = {
    val k = getKByAccuracy(accuracy)
    k / userSet.take(k).max
  }
}

object AspectAttributeActor {
  def props(): Props = Props(classOf[AspectAttributeActor])
}

case class CardinalityRequest(accuracy: Float)
case class CardinalityResponse(cardinality: Int, accuracy: Float)
