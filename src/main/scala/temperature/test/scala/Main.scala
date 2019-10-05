package temperature.test.scala

import java.time.LocalDate
import scala.io.Source
import scala.util.Try
import com.github.tototoshi.csv._
import temperature.test.scala.model.{MeteoRecord, MonthlyAverage}

object Main extends App {

  val startTime = System.nanoTime()

  println("- The avg temperature per month from hottest month to coldest month:" +
    System.lineSeparator() +
    withRecordsIterator(calculateAvgTemperatureByMonth).mkString(System.lineSeparator()))

  println("- The high temperature per month from hottest month to coldest month:" +
    System.lineSeparator() +
    withRecordsIterator(calculateMaxTemperatureByMonth).mkString(System.lineSeparator()))

  println("- The high temperature per month from hottest month to coldest month for 'Michigan' state:" +
    System.lineSeparator() +
    withRecordsIterator(ri => calculateMaxTemperatureByMonthForState(ri, "Michigan")).mkString(System.lineSeparator()))

  println("- The high temperature per month from hottest month to coldest month for 'Baltimore' country:" +
    System.lineSeparator() +
    withRecordsIterator(ri => calculateMaxTemperatureByMonthForCountry(ri, "Baltimore")).mkString(System.lineSeparator()))

  println("- How many days were temperatures above 75ºF: " + withRecordsIterator(hotDaysCount(_, 75)))

  println("- Seq of all available countries grouped by state" +
    System.lineSeparator() +
    withRecordsIterator(getSeqOfAllAvailableCounties).mkString(System.lineSeparator()))

  val endTime = System.nanoTime()
  println("Executed time: " + (endTime - startTime) + "ns")

  /** Parse sequence of string to the MeteoRecord.
   *
   * @param str - sequence of fields as parsed strings
   * @return a valid instance of MeteoRecord as Some, or None otherwise.
   */
  def toRecord(str: Seq[String]): Option[MeteoRecord] = {
    Try {
      val date = str(11)
      val latitude = str(5)
      val longitude = str(6)
      val measurement = str(13)
      val stateName = str(21)
      val countyName = str(22)

      MeteoRecord(
        LocalDate.parse(date),
        latitude.toDouble,
        longitude.toDouble,
        RecordService.extractDouble(measurement),
        stateName,
        countyName
      )
    }.toOption
  }

  /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly average temperatures from hottest to coldest.
   */
  def calculateAvgTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyAverage] = {
    def countAvgTemperature(groupedRecords: Map[Int, Seq[MeteoRecord]]): Seq[MonthlyAverage] = {
      groupedRecords.map(x => {
        val filteredData = x._2.filter(_.measurement.isDefined)
        val averageTemperatures = filteredData.foldLeft(0.0)((x, y) => x + y.measurement.get) / filteredData.length
        MonthlyAverage(x._1, averageTemperatures)
      }).toSeq
    }

    countAvgTemperature(RecordService.groupRecordsByMonth(records))
      .sortBy(_.avg)(Ordering[Double].reverse)
  }

  /** Monthly high temperature calculation for all data, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly high temperatures from hottest to coldest.
   */
  def calculateMaxTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MeteoRecord] = {
    RecordService
      .calculateMaxTemperatureByMonthWithFilter(records, RecordService.emptyFilter)
  }

  /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly high temperatures from hottest to coldest.
   */
  def calculateMaxTemperatureByMonthForState(records: Iterator[MeteoRecord], state: String): Seq[MeteoRecord] = {
    RecordService
      .calculateMaxTemperatureByMonthWithFilter(records, RecordService.stateFilter(state))
  }

  /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly high temperatures from hottest to coldest.
   */
  def calculateMaxTemperatureByMonthForCountry(records: Iterator[MeteoRecord], country: String): Seq[MeteoRecord] = {
    RecordService
      .calculateMaxTemperatureByMonthWithFilter(records, RecordService.countryFilter(country))
  }

  /** Counts how many days the temperature was higher for threshold for all data
   *
   * @param records iterator of MeteoRecord
   * @param threshold threshold temperature
   * @return count of days with temperature above threshold
   */
  def hotDaysCount(records: Iterator[MeteoRecord], threshold: Double): Int = {
    records
      .flatMap(_.measurement)
      .count(_ > threshold)

//    records.foldLeft(0)((days, curr) =>
//      curr.measurement match {
//        case Some(measurement) if measurement > threshold => days + 1
//        case _ => days
//      }
//    )
  }

  /** Sequence of all available countries grouped by state.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly high temperatures from hottest to coldest.
   */
  def getSeqOfAllAvailableCounties(records: Iterator[MeteoRecord]): Map[String, Set[String]] = {
    // maybe .groupBy(_._1).mapValues(_.map(t => (t._2, t._3)))
    records.foldLeft(Map.empty[String, Set[String]])((acc, curr) =>
      acc + (curr.stateName -> (acc.getOrElse(curr.stateName, Set.empty[String]) + curr.countyName))
    )
  }

  def withRecordsIterator[A](f: Iterator[MeteoRecord] => A): A = {
    val reader = CSVReader.open(Source.fromInputStream(getClass().getClassLoader.getResourceAsStream("temperature.csv")))
    try {
      f(reader.iterator.flatMap(toRecord(_)))
    } finally {
      reader.close()
    }
  }

}
