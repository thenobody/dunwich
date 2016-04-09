package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor}
import net.thenobody.dunwich.model.{EmptySketch, Sketch, UserEvent}

import scala.collection.mutable
import scala.collection.immutable
import scala.util.hashing.MurmurHash3

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeActor extends Actor {
  val userSet: Sketch = EmptySketch()

  override def receive = {
    case UserEvent(uuid, _, _, _, _, _) =>
      userSet.addUser(uuid)

    case CardinalityRequest(accuracy) =>
      val cardinality = userSet.cardinality
      sender() ! CardinalityResponse(cardinality, accuracy)

    case SketchRequest(accuracy) =>
      sender() ! SketchResponse(userSet)
  }
}

object AspectAttributeActor {
  def props(): Props = Props(classOf[AspectAttributeActor])
}

case class CardinalityRequest(accuracy: Float)
case class CardinalityResponse(cardinality: Int, accuracy: Float)

case class SketchRequest(accuracy: Float)
case class SketchResponse(sketch: Sketch)
