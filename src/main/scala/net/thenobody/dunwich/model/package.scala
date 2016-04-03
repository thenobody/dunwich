package net.thenobody.dunwich

/**
 * Created by antonvanco on 27/03/2016.
 */
package object model {

  case class UserEvent(uuid: String, timestamp: Long, aspect1: Int, aspect2: Int, aspect3: Int, aspect4: Int)
  object UserEvent {
    def apply(
      uuid: String,
      timestamp: String,
      aspect1: String,
      aspect2: String,
      aspect3: String,
      aspect4: String
    ): UserEvent = new UserEvent(uuid, timestamp.toLong, aspect1.toInt, aspect2.toInt, aspect3.toInt, aspect4.toInt)
  }

  trait Aspect {
    def attribute: Int
  }
  case class Aspect1(attribute: Int) extends Aspect
  case class Aspect2(attribute: Int) extends Aspect
  case class Aspect3(attribute: Int) extends Aspect
  case class Aspect4(attribute: Int) extends Aspect
}
