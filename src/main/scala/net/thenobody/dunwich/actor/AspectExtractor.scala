package net.thenobody.dunwich.actor

import akka.actor.{Props, ActorRef, Actor}
import net.thenobody.dunwich.model._

/**
 * Created by antonvanco on 27/03/2016.
 */
trait AspectExtractor extends Actor {
  val router: ActorRef

  def extractAttribute(userEvent: UserEvent): Aspect

  def receive = {
    case userEvent: UserEvent =>
      router.forward(extractAttribute(userEvent) -> userEvent)
  }
}

class Aspect1ExtractorActor(val router: ActorRef) extends AspectExtractor {
  def extractAttribute(userEvent: UserEvent): Aspect = Aspect1(userEvent.aspect1)
}
object Aspect1ExtractorActor {
  def props(router: ActorRef): Props = Props(classOf[Aspect1ExtractorActor], router)
}

class Aspect2ExtractorActor(val router: ActorRef) extends AspectExtractor {
  def extractAttribute(userEvent: UserEvent): Aspect = Aspect2(userEvent.aspect2)
}
object Aspect2ExtractorActor {
  def props(router: ActorRef): Props = Props(classOf[Aspect2ExtractorActor], router)
}

class Aspect3ExtractorActor(val router: ActorRef) extends AspectExtractor {
  def extractAttribute(userEvent: UserEvent): Aspect = Aspect3(userEvent.aspect3)
}
object Aspect3ExtractorActor {
  def props(router: ActorRef): Props = Props(classOf[Aspect3ExtractorActor], router)
}

class Aspect4ExtractorActor(val router: ActorRef) extends AspectExtractor {
  def extractAttribute(userEvent: UserEvent): Aspect = Aspect4(userEvent.aspect4)
}
object Aspect4ExtractorActor {
  def props(router: ActorRef): Props = Props(classOf[Aspect4ExtractorActor], router)
}
