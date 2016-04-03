package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor, ActorRef}
import net.thenobody.dunwich.model._

import scala.collection.mutable

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeRouter extends Actor {

  val aspectAttributes: mutable.Map[Aspect, ActorRef] = mutable.Map()

  def receive = {
    case (attribute: Aspect, userEvent: UserEvent) =>
      aspectAttributes.getOrElseUpdate(attribute, context.actorOf(AspectAttributeActor.props())) ! userEvent

    case (aspect: Aspect, request: CardinalityRequest) =>
      aspectAttributes.get(aspect) match {
        case Some(actor) => actor.forward(request)
        case None => sender() ! NoDataResponse(aspect)
      }

    case (aspect: Aspect, request: SampleRequest) =>
      aspectAttributes.get(aspect) match {
        case Some(actor) => actor.forward(request)
        case None => sender() ! NoDataResponse(aspect)
      }
  }
}

object AspectAttributeRouter {
  def props(): Props = Props(classOf[AspectAttributeRouter])
}

case class NoDataResponse(aspect: Aspect)
