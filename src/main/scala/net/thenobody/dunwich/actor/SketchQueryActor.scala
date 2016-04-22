package net.thenobody.dunwich.actor

import akka.actor.{Props, Actor, ActorRef}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import net.thenobody.dunwich.model._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Created by antonvanco on 03/04/2016.
 */
class SketchQueryActor(aspectAttributeRouter: ActorRef) extends Actor {

  type SampleHashes = immutable.SortedSet[Float]

  import context.dispatcher

  implicit val timeout = Timeout(3 seconds)

  override def receive = {
    case AspectQuery(aspect) =>
      aspectAttributeRouter ? (aspect -> SketchRequest) pipeTo sender()

    case OrQuery(terms) =>
      val futures = terms.map { term => self ? term map { case SketchResponse(sketch) => sketch } }
      Future.reduce(futures) (_ union _) map SketchResponse pipeTo sender()

    case AndQuery(terms) =>
      val futures = terms.map { term => self ? term map { case SketchResponse(sketch) => sketch } }
      Future.reduce(futures) (_ intersect _) map SketchResponse pipeTo sender()
  }
}

object SketchQueryActor {
  def props(aspectAttributeRouter: ActorRef): Props = Props(classOf[SketchQueryActor], aspectAttributeRouter)
}
