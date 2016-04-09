package net.thenobody.dunwich.actor

import akka.actor.{Props, ActorRef, Actor}
import net.thenobody.dunwich.model.UserEvent
import net.thenobody.dunwich.util.CsvUtil

import scala.io.Source

/**
 * Created by antonvanco on 27/03/2016.
 */
class FileParserActor extends Actor {

  def receive = {
    case FileInputMessage(file, funnel) =>
      println(s"started loading from $file")
      Source.fromFile(file).getLines().foreach(_.split('\t').toList match {
        case uuid +: timestamp +: aspect1 +: aspect2 +: aspect3 +: aspect4 +: Nil =>
          funnel ! UserEvent(uuid, timestamp, aspect1, aspect2, aspect3, aspect4)
      })
      println("done")
  }
}

object FileParserActor {
  def props(): Props = Props(classOf[FileParserActor])
}

case class FileInputMessage(file: String, funnel: ActorRef)

