package temperature.test.scala

import java.time.LocalDate
import com.github.tototoshi.csv._
import temperature.test.scala.model.{MeteoRecord, MonthlyAverage}
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
  def toRecord(str: Seq[String]): Option[MeteoRecord] = {
    try {

      val date = str(11)
      val latitude = str(5)
      val longitude = str(6)
      val measurement = str(13)
      val stateName = str(21)
      val countyName = str(22)

      Some(
        MeteoRecord(
          LocalDate.parse(date),
          latitude.toDouble,
          longitude.toDouble,
          extractDouble(measurement),
          stateName,
          countyName
        )
      )
    } catch {
      case _: Throwable => None
    }
  }

  /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly average temperatures from hottest to coldest.
   */
  def calculateAvgTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyAverage] = {
    def groupRecordsByMonth(records: Iterator[MeteoRecord]): Map[Int, Seq[MeteoRecord]] = {
      records.foldLeft(Map.empty[Int, Seq[MeteoRecord]])((acc, curr) =>
        curr.measurement match {
          case Some(_) =>
            acc + (curr.date.getMonthValue -> (acc.getOrElse(curr.date.getMonthValue, Seq.empty[MeteoRecord]) :+ curr))
          case None => acc
        }
      )
    }

    def countAvgTemperature(groupedRecords: Map[Int, Seq[MeteoRecord]]): Seq[MonthlyAverage] = {
      groupedRecords.map(x => {
        val filteredData = x._2.filter(x => x.measurement.isDefined)
        val averageTemperatures = filteredData.foldLeft(0.0)((x, y) => x + y.measurement.get) / filteredData.length
        MonthlyAverage(x._1, averageTemperatures)
      }).toSeq
    }

    countAvgTemperature(groupRecordsByMonth(records)).sortBy(_.avg)(Ordering[Double].reverse)
  }

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
