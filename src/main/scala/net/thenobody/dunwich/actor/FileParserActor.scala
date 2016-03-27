package net.thenobody.dunwich.actor

import akka.actor.{Props, ActorRef, Actor}
import net.thenobody.dunwich.util.CsvUtil

import scala.io.Source

/**
 * Created by antonvanco on 27/03/2016.
 */
class FileParserActor extends Actor {

  def receive = {
    case FileInputMessage(file, funnel) =>
      Source.fromFile(file).getLines().foreach(CsvUtil.parseCsv(_, '\t') match {
        case uuid +: timestamp +: aspect1 +: aspect2 +: aspect3 +: aspect4 +: Nil =>
          funnel ! UserEvent(uuid, timestamp, aspect1, aspect2, aspect3, aspect4)
      })
  }
}

object FileParserActor {
  def props(): Props = Props(classOf[FileParserActor])
}

case class FileInputMessage(file: String, funnel: ActorRef)
case class UserEvent(uuid: String, timestamp: Long, aspect1: Int, aspect2: Int, aspect3: Int, aspect4: Int)
object UserEvent {
  def apply(
    uuid: String,
    timestamp: String,
    aspect1: String,
    aspect2: String,
    aspect3: String,
    aspect4: String
  ): UserEvent = UserEvent(uuid, timestamp.toLong, aspect1.toInt, aspect2.toInt, aspect3.toInt, aspect4.toInt)
}
