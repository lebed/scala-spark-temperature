import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "temperature",
    libraryDependencies ++= backendDeps,
    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
    mainClass in (Compile, run) := Some("temperature.test.scala.Main")
  )