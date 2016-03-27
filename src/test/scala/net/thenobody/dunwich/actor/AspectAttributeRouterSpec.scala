package net.thenobody.dunwich.actor

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import net.thenobody.dunwich.util.UUIDUtil
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._

/**
 * Created by antonvanco on 27/03/2016.
 */
class AspectAttributeRouterSpec extends TestKit(ActorSystem()) with ImplicitSender with FlatSpecLike with Matchers {

  behavior of classOf[AspectAttributeRouter].getSimpleName

  it should "partition UserEvents by aspect attributes and delegate requests" in {
    val instance = system.actorOf(AspectAttributeRouter.props())

    val timestamp = 1234567890L
    val aspectPlaceholder = 1

    val count1 = 100000
    val count2 = 50000
    val count3 = 75000

    val aspectAttribute1 = 1
    val aspectAttribute2 = 2
    val aspectAttribute3 = 3

    val aspectAttributeNoData = 9

    (1 to count1).foreach { seed =>
      instance ! buildUserEventWitAttribute(aspectAttribute1, UUIDUtil.seededUUID(seed), timestamp, aspectPlaceholder)
    }

    (1 to count2).foreach { seed =>
      instance ! buildUserEventWitAttribute(aspectAttribute2, UUIDUtil.seededUUID(seed), timestamp, aspectPlaceholder)
    }

    (1 to count3).foreach { seed =>
      instance ! buildUserEventWitAttribute(aspectAttribute3, UUIDUtil.seededUUID(seed), timestamp, aspectPlaceholder)
    }

    instance ! (aspectAttribute1, CardinalityRequest(0.95F))
    val response1 = receiveOne(3 seconds)

    instance ! (aspectAttribute2, CardinalityRequest(0.95F))
    val response2 = receiveOne(3 seconds)

    instance ! (aspectAttribute3, CardinalityRequest(0.95F))
    val response3 = receiveOne(3 seconds)

    instance ! (aspectAttributeNoData, CardinalityRequest(0.95F))
    val response4 = receiveOne(3 seconds)

    response1 shouldBe a [CardinalityResponse]
    response1.asInstanceOf[CardinalityResponse].cardinality shouldBe count1 +- (count1 * 0.05F).toInt

    response2 shouldBe a [CardinalityResponse]
    response2.asInstanceOf[CardinalityResponse].cardinality shouldBe count2 +- (count2 * 0.05F).toInt

    response3 shouldBe a [CardinalityResponse]
    response3.asInstanceOf[CardinalityResponse].cardinality shouldBe count3 +- (count3 * 0.05F).toInt

    response4 shouldBe a [NoDataResponse]
  }

  def buildUserEventWitAttribute(attribute: Int, uuid: String, timestamp: Long, aspectPlaceholder: Int): (Int, UserEvent) = {
    (attribute, UserEvent(uuid, timestamp, aspectPlaceholder, aspectPlaceholder, aspectPlaceholder, aspectPlaceholder))
  }

}
