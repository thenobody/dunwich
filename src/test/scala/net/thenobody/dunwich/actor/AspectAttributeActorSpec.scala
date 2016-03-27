package net.thenobody.dunwich.actor

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import net.thenobody.dunwich.util.UUIDUtil
import org.scalatest.{Matchers, FlatSpecLike}

import scala.concurrent.duration._

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeActorSpec extends TestKit(ActorSystem()) with ImplicitSender with FlatSpecLike with Matchers {

  behavior of classOf[AspectAttributeActor].getSimpleName

  it should "store user events and estimate cardinality" in {
    val timestamp = 1234567890L
    val aspect = 1
    val instance = system.actorOf(AspectAttributeActor.props())

    val count = 100000
    (1 to count).foreach { seed =>
      instance ! UserEvent(UUIDUtil.seededUUID(seed), timestamp, aspect, aspect, aspect, aspect)
    }

    instance ! CardinalityRequest(0.90F)
    val response90 = receiveOne(3 seconds)

    response90 shouldBe a [CardinalityResponse]
    response90.asInstanceOf[CardinalityResponse].cardinality shouldBe count +- (0.10 * count).toInt

    instance ! CardinalityRequest(0.95F)
    val response95 = receiveOne(3 seconds)

    response95 shouldBe a [CardinalityResponse]
    response95.asInstanceOf[CardinalityResponse].cardinality shouldBe count +- (0.05 * count).toInt

    instance ! CardinalityRequest(0.98F)
    val response98 = receiveOne(3 seconds)

    response98 shouldBe a [CardinalityResponse]
    response98.asInstanceOf[CardinalityResponse].cardinality shouldBe count +- (0.02 * count).toInt
  }

}
