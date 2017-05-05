name := "prequelous"

version := "0.6"

organization := "info.hargrave"

scalaVersion := "2.12.2"

// Runtime Dependencies
libraryDependencies ++= Seq(
    "commons-lang"  % "commons-lang"    % "2.6",
    "joda-time"     % "joda-time"       % "2.9.9",
    "org.joda"      % "joda-convert"    % "1.8.1",
    "org.slf4j"     % "slf4j-api"       % "1.7.25",
    "com.zaxxer"    % "HikariCP"        % "2.6.1",
    "org.javassist" % "javassist"       % "3.21.0-GA"
)

// Test Dependencies
libraryDependencies ++= Seq(
    "org.hsqldb"    %  "hsqldb"     % "2.2.4" % "test",
    "org.scalactic" %% "scalactic"  % "3.0.3" % "test",
    "org.scalatest" %% "scalatest"  % "3.0.3" % "test"
)


