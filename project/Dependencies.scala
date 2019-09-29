import sbt._

object Dependencies {
  lazy val scalaCsv = "com.github.tototoshi" %% "scala-csv" % "1.3.5"

  lazy val scalactic = "org.scalactic" %% "scalactic" % "3.0.8"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
}
