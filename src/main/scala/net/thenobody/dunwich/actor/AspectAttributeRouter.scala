package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor, ActorRef}

import scala.collection.mutable

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeRouter extends Actor {

  val aspectAttributes: mutable.Map[Int, ActorRef] = mutable.Map()

  def receive = {
    case (attribute: Int, userEvent: UserEvent) =>
      aspectAttributes.getOrElseUpdate(attribute, context.actorOf(AspectAttributeActor.props())) ! userEvent
    case (attribute: Int, request: CardinalityRequest) =>
      aspectAttributes.get(attribute) match {
        case Some(actor) => actor.forward(request)
        case None => sender() ! NoDataResponse(attribute)
      }
  }

}

object AspectAttributeRouter {
  def props(): Props = Props(classOf[AspectAttributeRouter])
}

case class NoDataResponse(attribute: Int)
