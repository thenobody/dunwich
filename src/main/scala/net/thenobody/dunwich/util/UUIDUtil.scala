package net.thenobody.dunwich.util

import java.util.UUID

/**
 * Created by antonvanco on 27/03/2016.
 */
object UUIDUtil {

  def randomUUID: String = UUID.randomUUID.toString

  def seededUUID(seed: Any): String = UUID.nameUUIDFromBytes(seed.toString.getBytes).toString

}
