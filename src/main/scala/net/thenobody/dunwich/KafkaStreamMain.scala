package net.thenobody.dunwich

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.kafka.ConsumerSettings
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.Timeout
import net.thenobody.dunwich.actor.stream.UserEventActorSubscriber
import net.thenobody.dunwich.actor.{AspectAttributeRouter, SketchQueryActor, SketchResponse}
import net.thenobody.dunwich.model._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import spray.json._

import scala.concurrent.duration._

/**
  * Created by antonvanco on 21/04/2016.
  */
object KafkaStreamMain {

  val UserEventsTopic = "user-events"
  val bootstrapServers = Seq("localhost:9092").mkString(",")
  val ConsumerGroupId = "user-events-akka-streams"

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    val aspectAttributeRouter = system.actorOf(AspectAttributeRouter.props())
    val sketchQueryActor = system.actorOf(SketchQueryActor.props(aspectAttributeRouter))

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer,
      Set(UserEventsTopic))
      .withBootstrapServers(bootstrapServers)
      .withGroupId(ConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

//    val graph = Consumer.plainSource(consumerSettings)
    val start = System.currentTimeMillis
    val graph = Source.fromIterator(() => scala.io.Source.fromFile("/Users/antonvanco/develop/data/user-events/randomised.txt").getLines())
      .map { message =>
        UserEvent(message.split('\t').toList)
      }.alsoToMat(Sink.fold(0) { (acc, _) =>
        if (acc % 1000000 == 0) println(s"processed $acc messages")
        acc + 1
      }) (Keep.right)
      .to(Sink.actorSubscriber(UserEventActorSubscriber.props(aspectAttributeRouter)))

    println("starting")
    graph.run() onSuccess {
      case count =>
        println(s"processed $count messages")
        println(s"time: ${(System.currentTimeMillis - start) / 1000.0}")
    }

    Http().bindAndHandle(HttpService(sketchQueryActor), "localhost", 8080)
  }
}


case class CardinalityJson(cardinality: Int, variance: Double, mean: Double, min: Float, max: Float)
object CardinalityJson {
  def apply(sketch: BoundedSketch): CardinalityJson = CardinalityJson(
    sketch.cardinality,
    sketch.variance,
    sketch.mean,
    sketch.hashes.min,
    sketch.hashes.max
  )
}

trait JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val cardinalityJsonFormat = jsonFormat5(CardinalityJson.apply)

  implicit object QueryProtocol extends RootJsonFormat[Query] {
    override def write(obj: Query): JsValue = ???

    override def read(json: JsValue): Query = {
      json.asJsObject.fields.map {
        case ("and", JsArray(jsValues)) => AndQuery(jsValues.map(read))
        case ("or", JsArray(jsValues)) => OrQuery(jsValues.map(read))
        case (aspect, JsNumber(attribute)) => AspectQuery(Aspect(aspect, attribute.toInt))
      }.head
    }
  }
}

object HttpService extends JsonProtocol {
  implicit val timeout = Timeout(5 seconds)

  def apply(sketchQueryActor: ActorRef) = {
    path("v1" / "cardinality") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
      } ~
      post {
        entity(as[Query]) { postQuery =>
          println(postQuery)
          onSuccess(sketchQueryActor ? postQuery ) { case SketchResponse(sketch) =>
            complete(CardinalityJson(sketch))
          }
        }
      }
    }
  }
}
