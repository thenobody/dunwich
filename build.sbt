name := "dunwich"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val log4jVersion = "2.4.1"
  val akkaVersion = "2.4.2"
  Seq(
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test"
  )
}
    