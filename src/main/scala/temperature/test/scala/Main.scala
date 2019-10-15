package temperature.test.scala

import java.time.LocalDate

import scala.io.Source
import scala.util.Try
import com.github.tototoshi.csv._
import temperature.test.Messages
import temperature.test.scala.model.{MeteoRecord, MonthlyMeasurement}

object Main extends App {

  val startTime = System.nanoTime()

  println(Messages.MIN_TEMPERATURE_BY_MONTH + System.lineSeparator() +
    withRecordsIterator(calculateMinTemperatureByMonth).mkString(System.lineSeparator()))

  println(Messages.AVG_TEMPERATURE_BY_MONTH + System.lineSeparator() +
    withRecordsIterator(calculateAvgTemperatureByMonth).mkString(System.lineSeparator()))

  println(Messages.MAX_TEMPERATURE_BY_MONTH + System.lineSeparator() +
    withRecordsIterator(calculateMaxTemperatureByMonth).mkString(System.lineSeparator()))


  println(Messages.MIN_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
    withRecordsIterator(ri => calculateMinTemperatureForStateByMonth(ri, "Michigan")).mkString(System.lineSeparator()))

  println(Messages.AVG_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
    withRecordsIterator(ri => calculateAvgTemperatureForStateByMonth(ri, "Michigan")).mkString(System.lineSeparator()))

  println(Messages.MAX_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
    withRecordsIterator(ri => calculateMaxTemperatureForStateByMonth(ri, "Michigan")).mkString(System.lineSeparator()))


  println(Messages.MIN_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
    withRecordsIterator(ri => calculateMinTemperatureForCountryByMonth(ri, "Baltimore")).mkString(System.lineSeparator()))

  println(Messages.AVG_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
    withRecordsIterator(ri => calculateAvgTemperatureForCountryByMonth(ri, "Baltimore")).mkString(System.lineSeparator()))

  println(Messages.MAX_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
    withRecordsIterator(ri => calculateMaxTemperatureForCountryByMonth(ri, "Baltimore")).mkString(System.lineSeparator()))


  println(Messages.ALL_RECORDS_WITH_MAX_TEMPERATURE_FOR_EVERY_MONTH + System.lineSeparator() +
    withRecordsIterator(findAllRecordWithMaxTemperatureForEveryMonth).mkString(System.lineSeparator()))


  println(Messages.HOT_DAYS + withRecordsIterator(hotDaysCount(_, 75)))

  println(Messages.ALL_AVAILABLE_COUNTRIES + System.lineSeparator() +
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


  /** Monthly min temperature calculation, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly average temperatures from hottest to coldest.
   */
  def calculateMinTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.min

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.emptyFilter, aggregate)
  }

  /** Monthly average temperature calculation, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly average temperatures from hottest to coldest.
   */
  def calculateAvgTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.sum / measurements.length

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.emptyFilter, aggregate)
  }

  /** Monthly high temperature calculation, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @return sequence of monthly high temperatures from hottest to coldest.
   */
  def calculateMaxTemperatureByMonth(records: Iterator[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.max

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.emptyFilter, aggregate)
  }

  /** Monthly min temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param state state name String
   * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMinTemperatureForStateByMonth(records: Iterator[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.min

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.stateFilter(state), aggregate)
  }

  /** Monthly average temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param state state name String
   * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
   */
  def calculateAvgTemperatureForStateByMonth(records: Iterator[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.sum / measurements.length

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.stateFilter(state), aggregate)
  }

  /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param state state name String
   * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMaxTemperatureForStateByMonth(records: Iterator[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.max

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.stateFilter(state), aggregate)
  }

  /** Monthly min temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param country country name String
   * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMinTemperatureForCountryByMonth(records: Iterator[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.min

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.countryFilter(country), aggregate)
  }

  /** Monthly average temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param country country name String
   * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
   */
  def calculateAvgTemperatureForCountryByMonth(records: Iterator[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.sum / measurements.length

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.countryFilter(country), aggregate)
  }

  /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records iterator of MeteoRecord
   * @param country country name String
   * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMaxTemperatureForCountryByMonth(records: Iterator[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.max

    RecordService
      .calculateTemperatureWithFilterByMonth(records, RecordService.countryFilter(country), aggregate)
  }

  /** Find all records with the maximum temperatures for every month.
   * If there were several days with a maximum temperature in a month, then get everything.
   *
   * @param records meteo records Dataset
   * @return sequence of meteo records ordered by temperatures from hottest to coldest
   */
  def findAllRecordWithMaxTemperatureForEveryMonth(records: Iterator[MeteoRecord]): Seq[MeteoRecord] = {
    val aggregate: Seq[Double] => Double = measurements => measurements.max

    RecordService
      .calculateMaxTemperatureWithFilterByMonth(records, aggregate)
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

//  alternative way
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
