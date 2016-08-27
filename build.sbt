name := "prequelous"

version := "0.5"

organization := "it.vannicaruso"

scalaVersion := "2.11.7"

// Runtime Dependencies
libraryDependencies ++= Seq(
    "commons-lang" % "commons-lang" % "2.6",
    "joda-time" % "joda-time" % "2.2",
    "org.joda" % "joda-convert" % "1.3.1",
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "com.zaxxer" % "HikariCP" % "1.3.8",
    "org.javassist" % "javassist" % "3.18.1-GA"

)

// Test Dependencies
libraryDependencies ++= Seq(
    "org.hsqldb" % "hsqldb" % "2.2.4" % "test",
    "org.scalactic" %% "scalactic" % "2.2.6" % "test",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)


