package net.thenobody.dunwich

import akka.actor.{Actor, Props, ActorSystem}
import net.thenobody.dunwich.actor.{UserEvent, FileInputMessage, FileParserActor}
import org.slf4j.LoggerFactory

/**
  * Created by antonvanco on 23/03/2016.
  */
object Main {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()

    val fileParser = system.actorOf(FileParserActor.props())
    fileParser ! FileInputMessage(
      "/tmp/test.input",
      system.actorOf(Props(new Actor {
        def receive = {
          case userEvent: UserEvent =>
            logger.info(userEvent.toString)
        }
      }))
    )
  }

}
