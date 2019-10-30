name := "missjackson"

version := "2019.8.1"

scalaVersion := "2.13.0"
lazy val versions = new {
  val jackson = "2.9.9"
}

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % versions.jackson,
  "com.fasterxml.jackson.core" % "jackson-databind" % versions.jackson,

  "org.scalatest"  %% "scalatest"         % "3.0.8"  % Test
)
