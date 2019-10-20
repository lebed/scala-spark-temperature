package temperature.test.spark

import java.sql.{Date, Timestamp}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import temperature.test.Messages
import temperature.test.spark.model.{MeteoRecord, MonthlyMeasurement}

object Entry {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder.appName("Temperature Test")
    .config("spark.master", "local")
    .getOrCreate()

  var records: Dataset[MeteoRecord] = _
  import spark.implicits._

  def main(args: Array[String]) {
    /**
     * Dataset of temperature.csv
     */
    records = {
      spark
        .read
        .options(Map(
          "header" -> "true",
          "inferSchema" -> "true"
        ))
        .csv("src/main/resources/temperature.csv")
        .map(field => {
          val longitude = field.getAs[Double]("longitude")
          val latitude = field.getAs[Double]("latitude")
          val timestamp = field.getAs[Timestamp]("date_gmt")
          val measurement = field.getAs[Double]("sample_measurement")
          val stateName = field.getAs[String]("state_name")
          val countryName = field.getAs[String]("county_name")

          MeteoRecord(
            new Date(timestamp.getTime),
            latitude,
            longitude,
            Some(measurement),
            stateName,
            countryName
          )
        })
    }


    println(Messages.MIN_TEMPERATURE_BY_MONTH + System.lineSeparator() +
      calculateMinTemperatureByMonth(records).mkString(System.lineSeparator()))

    println(Messages.AVG_TEMPERATURE_BY_MONTH + System.lineSeparator() +
      calculateAvgTemperatureByMonth(records).mkString(System.lineSeparator()))

    println(Messages.MAX_TEMPERATURE_BY_MONTH + System.lineSeparator() +
      calculateMaxTemperatureByMonth(records).mkString(System.lineSeparator()))


    println(Messages.MIN_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
      calculateMinTemperatureForStateByMonth(records, "Michigan").mkString(System.lineSeparator()))

    println(Messages.AVG_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
      calculateAvgTemperatureForStateByMonth(records, "Michigan").mkString(System.lineSeparator()))

    println(Messages.MAX_TEMPERATURE_BY_MONTH_FOR_STATE + System.lineSeparator() +
      calculateMaxTemperatureForStateByMonth(records, "Michigan").mkString(System.lineSeparator()))


    println(Messages.MIN_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
      calculateMinTemperatureForCountryByMonth(records, "Baltimore").mkString(System.lineSeparator()))

    println(Messages.AVG_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
      calculateAvgTemperatureForCountryByMonth(records, "Baltimore").mkString(System.lineSeparator()))

    println(Messages.MAX_TEMPERATURE_BY_MONTH_FOR_COUNTRY + System.lineSeparator() +
      calculateMaxTemperatureForCountryByMonth(records, "Baltimore").mkString(System.lineSeparator()))


    println(Messages.ALL_RECORDS_WITH_MAX_TEMPERATURE_FOR_EVERY_MONTH + System.lineSeparator() +
      findAllRecordWithMaxTemperatureForEveryMonth(records).mkString(System.lineSeparator()))


    println(Messages.HOT_DAYS + hotDaysCount(records, 75))

    println(Messages.ALL_AVAILABLE_COUNTRIES + System.lineSeparator() +
      getSeqOfAllAvailableCounties(records).mkString(System.lineSeparator()))

  }


  /** Monthly min temperature calculation, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMinTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val minMeasurementFilter = min("measurement")
    groupTemperatureWithFilterByMonth(records, emptyFilter, minMeasurementFilter)
  }

  /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
   *
   * @param records meteo records Dataset
   * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
   */
  def calculateAvgTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val avgMeasurementFilter = avg("measurement")
    groupTemperatureWithFilterByMonth(records, emptyFilter, avgMeasurementFilter)
  }

  /** Monthly high temperature calculation, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMaxTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
    val maxMeasurementFilter = max("measurement")
    groupTemperatureWithFilterByMonth(records, emptyFilter, maxMeasurementFilter)
  }

  /** Monthly min temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param state state name String
   * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMinTemperatureForStateByMonth(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = min("measurement")
    val filter = stateFilter(state)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Monthly average temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param state state name String
   * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
   */
  def calculateAvgTemperatureForStateByMonth(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = avg("measurement")
    val filter = stateFilter(state)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param state state name String
   * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMaxTemperatureForStateByMonth(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = max("measurement")
    val filter = stateFilter(state)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Monthly min temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param country country name String
   * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMinTemperatureForCountryByMonth(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = min("measurement")
    val filter = countryFilter(country)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Monthly average temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param country country name String
   * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
   */
  def calculateAvgTemperatureForCountryByMonth(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = avg("measurement")
    val filter = countryFilter(country)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
   *
   * @param records meteo records Dataset
   * @param country country name String
   * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
   */
  def calculateMaxTemperatureForCountryByMonth(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
    val aggregateColumn = max("measurement")
    val filter = countryFilter(country)

    groupTemperatureWithFilterByMonth(records, filter, aggregateColumn)
  }

  /** Find all records with the maximum temperatures for every month.
   * If there were several days with a maximum temperature in a month, then display everything.
   *
   * @param records meteo records Dataset
   * @return sequence of meteo records ordered by temperatures from hottest to coldest
   */
  def findAllRecordWithMaxTemperatureForEveryMonth(records: Dataset[MeteoRecord]): Seq[MeteoRecord] = {
    val filter = emptyFilter
    val aggregateColumn = max("measurement")

    val filteredRecords = records
      .filter(filter)
      .cache()

    val groupedRecordsByMonth = filteredRecords
      .withColumn("month", month($"date"))
      .groupBy('month)
      .agg(aggregateColumn as "aggregateMeasurement")

    filteredRecords
      .join(groupedRecordsByMonth,
        groupedRecordsByMonth.col("aggregateMeasurement") === records.col("measurement") &&
          groupedRecordsByMonth.col("month") === month(records.col("date"))
      )
      .dropDuplicates()
      .drop("aggregateMeasurement", "month")
      .orderBy(desc("measurement"))
      .as[MeteoRecord]
      .collect()
  }

  /** Counts how many days the temperature was higher for threshold for all data.
   *
   * @param records meteo records Dataset
   * @param threshold threshold temperature
   * @return count of days with temperature above threshold
   */
  def hotDaysCount(records: Dataset[MeteoRecord], threshold: Double): Long = {
    records
      .filter(_.measurement match {
        case Some(v) if v > threshold => true
        case _ => false
      }).count()
  }

  /** Sequence of all available countries grouped by state.
   *
   * @param records meteo records Dataset
   * @return map includes keys as a state name and values as country names
   */
  def getSeqOfAllAvailableCounties(records: Dataset[MeteoRecord]): Map[String, Set[String]] = {
    records
      .groupBy('stateName)
      .agg(collect_list("countryName") as "countryNames")
      .as[(String, Set[String])]
      .collect()
      .toMap
  }


  type MeteoRecordFilter = MeteoRecord => Boolean
  def emptyFilter: MeteoRecordFilter = _ => true
  def countryFilter: String => MeteoRecordFilter = countryName => record => record.countryName == countryName
  def stateFilter: String => MeteoRecordFilter = stateName => record => record.stateName == stateName

  def groupTemperatureWithFilterByMonth(
      records: Dataset[MeteoRecord],
      filter: MeteoRecordFilter,
      aggregateColumn: Column
    ): Seq[MonthlyMeasurement] = {
      records
        .filter(filter)
        .withColumn("month", month($"date"))
        .groupBy("month")
        .agg(aggregateColumn.as("measurementTempByMonth"))
        .map(x => {
          MonthlyMeasurement(x.getAs[Int]("month"), round(x.getAs[Double]("measurementTempByMonth")))
        })
        .orderBy(desc("measurement"))
        .collect()
  }

  def round(d: Double): Double = {
    BigDecimal(d).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

}
