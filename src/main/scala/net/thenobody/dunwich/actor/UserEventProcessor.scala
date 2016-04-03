package net.thenobody.dunwich.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{BroadcastRoutingLogic, Router}
import net.thenobody.dunwich.model._

/**
 * Created by antonvanco on 27/03/2016.
 */
class UserEventProcessor(aspectAttributeRouter: ActorRef) extends Actor {
  val extractorRouter = {
    Router(BroadcastRoutingLogic())
      .addRoutee(context.actorOf(Aspect1ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect2ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect3ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect4ExtractorActor.props(aspectAttributeRouter)))
  }

  def receive = {
    case userEvent: UserEvent =>
      extractorRouter.route(userEvent, sender())
  }
}

object UserEventProcessor {
  def props(aspectAttributeRouter: ActorRef): Props = Props(classOf[UserEventProcessor], aspectAttributeRouter)
}
