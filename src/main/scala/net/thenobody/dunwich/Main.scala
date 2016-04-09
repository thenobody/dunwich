package net.thenobody.dunwich

import java.io.File

import akka.actor.{Status, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import net.thenobody.dunwich.actor._
import net.thenobody.dunwich.model._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by antonvanco on 23/03/2016.
  */
object Main {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()

    val aspectAttributeRouter = system.actorOf(AspectAttributeRouter.props())
    val userEventProcessor = system.actorOf(UserEventProcessor.props(aspectAttributeRouter))
    val sketchQueryActor = system.actorOf(SketchQueryActor.props(aspectAttributeRouter))

    val inputs = new File("/Users/antonvanco/develop/data/user-events/large").listFiles
    inputs.grouped(inputs.size / 7).foreach { files =>
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
        println("terms count:")
        val terms = (1 to io.StdIn.readInt).map(readAndParseAspect).map(AspectQuery)
        val query = io.StdIn.readLine("op:\n") match {
          case "and" => AndQuery(terms)
          case "or" => OrQuery(terms)
        }
        sketchQueryActor ? (accuracy -> query) onSuccess {
          case SketchResponse(sketch) =>
            println(s"cardinality: ${sketch.cardinality}")
        }
      }
    }

    def readAndParseAspect(num: Int): Aspect = io.StdIn.readLine(s"($num) aspect:").split("=").toList match {
      case "1" +: attribute +: Nil => Aspect1(attribute.toInt)
      case "2" +: attribute +: Nil => Aspect2(attribute.toInt)
      case "3" +: attribute +: Nil => Aspect3(attribute.toInt)
      case "4" +: attribute +: Nil => Aspect4(attribute.toInt)
    }
  }

}
