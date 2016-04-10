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
  val userSketch: Sketch = EmptySketch()

  override def receive = {
    case UserEvent(uuid, _, _, _, _, _) =>
      userSketch.addUser(uuid)
      sender() ! UserEventSketched

    case CardinalityRequest(accuracy) =>
      val cardinality = userSketch.cardinality(accuracy)
      sender() ! CardinalityResponse(cardinality, accuracy)

    case SketchRequest(accuracy) =>
      sender() ! SketchResponse(userSketch.sampled(accuracy))
  }
}

object AspectAttributeActor {
  def props(): Props = Props(classOf[AspectAttributeActor])
}

case object UserEventSketched

case class CardinalityRequest(accuracy: Float)
case class CardinalityResponse(cardinality: Int, accuracy: Float)

case class SketchRequest(accuracy: Float)
case class SketchResponse(sketch: Sketch)
