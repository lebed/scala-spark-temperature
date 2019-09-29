import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "temperature",
    libraryDependencies ++= Seq(
      scalaCsv,
      sparkSql,
      sparkCoreTest,
      sparkSqlTest,
      sparkCatalystTest,
      scalactic,
      scalaTest
    ),
    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
    mainClass in (Compile, run) := Some("temperature.test.scala.Main")
  )