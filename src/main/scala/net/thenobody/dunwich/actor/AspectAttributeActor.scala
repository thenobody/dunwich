package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor}

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeActor extends Actor {
  val userSet: mutable.SortedSet[Float] = mutable.SortedSet()
  override def receive = {
    case UserEvent(uuid, _, _, _, _, _) =>
      addUser(uuid)

    case CardinalityRequest(accuracy) =>
      val cardinality = estimateCardinality(accuracy)
      sender() ! CardinalityResponse(cardinality.toInt, accuracy)
  }

  def addUser(uuid: String): Unit = userSet += getUserHash(uuid)

  def getUserHash(uuid: String): Float = MurmurHash3.stringHash(uuid) match {
    case h if h < 0 => 0.5F - 0.5F * (h / Int.MinValue.toFloat)
    case h => 0.5F + 0.5F * (h / Int.MaxValue.toFloat)
  }

  def getKByAccuracy(accuracy: Float): Int = math.round(1 / math.pow(1.0 - accuracy, 2) + 2).toInt

  def estimateCardinality(accuracy: Float): Float = {
    val k = getKByAccuracy(accuracy)
    val sample = userSet.take(k)
    sample.size / sample.max
  }
}

object AspectAttributeActor {
  def props(): Props = Props(classOf[AspectAttributeActor])
}

case class CardinalityRequest(accuracy: Float)
case class CardinalityResponse(cardinality: Int, accuracy: Float)