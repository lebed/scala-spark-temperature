import sbt._

object Dependencies {
  lazy val scalaCsv = "com.github.tototoshi" %% "scala-csv" % "1.3.5"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.2"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.2"
  lazy val sparkCatalyst = "org.apache.spark" %% "spark-catalyst"

  lazy val scalactic = "org.scalactic" %% "scalactic" % "3.0.8"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
}