lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7",
  resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
)

lazy val util = (project in file("util")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "18.0",
      "com.github.nscala-time" %% "nscala-time" % "2.6.0",
      "com.ibm.icu" % "icu4j" % "56.1",
      "org.msgpack" %% "msgpack-scala" % "0.6.11",
      "org.json4s" %% "json4s-jackson" % "3.3.0",
      "com.typesafe" % "config" % "1.3.0",
      "log4j" % "log4j" % "1.2.17",
      "com.typesafe.akka" %% "akka-actor" % "2.4.1",
      "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
      "org.scalaj" %% "scalaj-http" % "1.1.4"
    ))

lazy val scindex = (project in file("scindex")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.rocksdb" % "rocksdbjni" % "3.9.0",
      "com.twitter" %% "util-collection" % "6.30.0"
    )).
  aggregate(util).dependsOn(
  util % "test->test;compile->compile"
)

lazy val comment = (project in file("comment")).
  settings(commonSettings: _*).
  settings(mainClass in assembly := Some("com.dedup.comment.Main")).
  aggregate(util, scindex).dependsOn(
  util % "test->test;compile->compile",
  scindex % "test->test;compile->compile"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  aggregate(comment).dependsOn(
  comment % "test->test;compile->compile"
)