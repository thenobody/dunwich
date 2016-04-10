package net.thenobody.dunwich

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.Timeout
import net.thenobody.dunwich.actor.{SketchQueryActor, SketchResponse, AspectAttributeRouter}
import net.thenobody.dunwich.actor.stream.UserEventActorSubscriber
import net.thenobody.dunwich.model._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Try

/**
 * Created by antonvanco on 10/04/2016.
 */
object StreamsMain {

  def splitLine(line: String): List[String] = line.split('\t').toList

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("stream")
    implicit val materializer = ActorMaterializer()

    val start = System.currentTimeMillis

    val aspectAttributeRouter = system.actorOf(AspectAttributeRouter.props())

    val inputPath = "/Users/antonvanco/develop/data/user-events/large/part-*"
    val linesSource = Source.single(inputPath)
      .map(new File(_))
      .mapConcat {
        case file if file.getName.contains("*") =>
          val namePattern = file.getName.replaceAll("\\*", ".*")
          file.getParentFile.listFiles.toList.filter(_.getName.matches(namePattern))
        case file if file.isFile =>
          List(file)
        case file =>
          file.listFiles.toList
      }.mapConcat { file =>
        println(s"opening file: $file")
        new immutable.Iterable[List[String]] {
          override def iterator = {
            val source = file match {
              case _ if file.getName.matches(".*\\.gz$") =>
                io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(file)))
              case _ =>
                io.Source.fromFile(file)
            }
            source.getLines().drop(1).map(splitLine)
          }
        }
      }.grouped(1000)
      .mapConcat(_.map(UserEvent.apply))
      .alsoToMat(Sink.fold(0) { case (count, _) =>
        if (count % 1000000 == 0) println(s"processed messages: $count")
        count + 1
      }) (Keep.right)
      .to(Sink.actorSubscriber(UserEventActorSubscriber.props(aspectAttributeRouter)))

    import system.dispatcher

    linesSource.run().onComplete { count =>
      val time = (System.currentTimeMillis - start) / 1000.0
      println(s"time: $time")
      count.foreach(println)
    }

    query()

    def query(): Unit = {
      implicit val timeout = Timeout(5 seconds)
      val sketchQueryActor = system.actorOf(SketchQueryActor.props(aspectAttributeRouter))

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
              println(s"variance: ${sketch.variance}")
              println(s"mean: ${sketch.mean}")
              println(s"min hash: ${sketch.hashes.min}")
              println(s"max hash: ${sketch.hashes.max}")
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


}
