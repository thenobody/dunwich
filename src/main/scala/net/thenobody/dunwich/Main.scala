package net.thenobody.dunwich

import java.io.File

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import net.thenobody.dunwich.actor._
import net.thenobody.dunwich.model.{Aspect4, Aspect3, Aspect2, Aspect1}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by antonvanco on 23/03/2016.
  */
object Main {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()

    val aspectAttributeRouter = system.actorOf(AspectAttributeRouter.props())
    val userEventProcessor = system.actorOf(UserEventProcessor.props(aspectAttributeRouter))

    new File("/Users/antonvanco/develop/data/user-events/split").listFiles.grouped(8).foreach { files =>
      val fileParser = system.actorOf(FileParserActor.props())
      files.foreach { file =>
        println(s"file ${file.getPath}")
        fileParser ! FileInputMessage(file.getPath, userEventProcessor)
      }
    }

    import system.dispatcher
    implicit val timeout = Timeout(5 seconds)
    Stream.from(1).foreach { _ =>
      Try {
        println("accuracy:")
        val accuracy = io.StdIn.readFloat()
        val aspect = io.StdIn.readLine("aspect:").split("=").toList match {
          case "1" +: attribute +: Nil => Aspect1(attribute.toInt)
          case "2" +: attribute +: Nil => Aspect2(attribute.toInt)
          case "3" +: attribute +: Nil => Aspect3(attribute.toInt)
          case "4" +: attribute +: Nil => Aspect4(attribute.toInt)
        }
        aspectAttributeRouter ?(aspect, CardinalityRequest(accuracy)) onSuccess {
          case CardinalityResponse(cardinality, _) =>
            println(s"aspect: $aspect")
            println(s"cardinality: $cardinality accuracy1: $accuracy")

        }
      }
    }
  }

}
