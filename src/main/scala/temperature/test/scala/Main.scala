package temperature.test.scala

import com.github.tototoshi.csv._
import temperature.test.model.{MeteoRecord, MonthlyAverage}

import scala.io.Source
import scala.util.Try

object Main extends App {

  val startTime = System.nanoTime()

  println("Months from hottest to coldest:" +
    System.lineSeparator() +
    withRecordsIterator(calculateAvgTemperatureByMonth).mkString(System.lineSeparator()))

  val endTime = System.nanoTime()
  println("Executed time: " + (endTime - startTime) + "ns")

  /** Parse sequence of string to the MeteoRecord.
   *
   * @param str - sequence of fields as parsed strings
   * @return a valid instance of MeteoRecord as Some, or None otherwise.
   */
  def toRecord(str: Seq[String]): Option[MeteoRecord] = ???

  /** Monthly average temperature calculation, sorted in decreasing order of temperature.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly average temperatures from hottest to coldest.
   */
  def calculateAvgTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyAverage] = ???

  def withRecordsIterator[A](f: Iterator[MeteoRecord] => A): A = {
    val reader = CSVReader.open(Source.fromInputStream(getClass().getClassLoader.getResourceAsStream("temperature.csv")))
    try {
      f(reader.iterator.flatMap(toRecord(_)))
    } finally {
      reader.close()
    }
  }

  def extractDouble(x: Any): Option[Double] = Try(x.toString.toDouble).toOption
}
