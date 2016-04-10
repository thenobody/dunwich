package net.thenobody.dunwich.actor.stream

import akka.actor.{ActorRef, Props}
import akka.routing.{BroadcastRoutingLogic, Router}
import akka.stream.actor.ActorSubscriberMessage._
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import net.thenobody.dunwich.actor._
import net.thenobody.dunwich.model.UserEvent

/**
 * Created by antonvanco on 10/04/2016.
 */
class UserEventActorSubscriber(aspectAttributeRouter: ActorRef) extends ActorSubscriber {
  import UserEventActorSubscriber._

  val extractorRouter = {
    Router(BroadcastRoutingLogic())
      .addRoutee(context.actorOf(Aspect1ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect2ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect3ExtractorActor.props(aspectAttributeRouter)))
      .addRoutee(context.actorOf(Aspect4ExtractorActor.props(aspectAttributeRouter)))
  }

  var currentInFlight = 0

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(MaxParallelUserEvents) {
    override def inFlightInternally: Int = currentInFlight
  }

  override def receive: Receive = {
    case OnNext(userEvent: UserEvent) =>
      currentInFlight += extractorRouter.routees.size
      extractorRouter.route(userEvent, self)

    case UserEventSketched =>
      currentInFlight -= 1
  }
}

object UserEventActorSubscriber {
  val MaxParallelUserEvents = 1024

  def props(aspectAttributeRouter: ActorRef): Props = Props(classOf[UserEventActorSubscriber], aspectAttributeRouter)
}
