package net.thenobody.dunwich.actor

import akka.actor.{Actor, Props}
import net.thenobody.dunwich.model.{BoundedSketch, UserEvent}

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeActor extends Actor {
  val userSketch: BoundedSketch = BoundedSketch()

  override def receive = {
    case UserEvent(uuid, _, _, _, _, _) =>
      userSketch.addUser(uuid)
      sender() ! UserEventSketched

    case CardinalityRequest(accuracy) =>
      val cardinality = userSketch.cardinality
      sender() ! CardinalityResponse(cardinality, accuracy)

    case SketchRequest(accuracy) =>
      sender() ! SketchResponse(userSketch)
  }
}

object AspectAttributeActor {
  def props(): Props = Props(classOf[AspectAttributeActor])
}

case object UserEventSketched

case class CardinalityRequest(accuracy: Float)
case class CardinalityResponse(cardinality: Int, accuracy: Float)

case class SketchRequest(accuracy: Float)
case class SketchResponse(sketch: BoundedSketch)
