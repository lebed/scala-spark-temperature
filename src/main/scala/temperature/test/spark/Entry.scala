package temperature.test.spark

import java.sql.{Date, Timestamp}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import temperature.test.spark.model.{MeteoRecord, MonthlyMeasurement}

object Entry {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Temperature Test")
      .config("spark.master", "local")
      .getOrCreate()

    /**
     * Dataset of temperature.csv
     */
    val records: Dataset[MeteoRecord] = {
      import spark.implicits._

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
          val countyName = field.getAs[String]("county_name")

          MeteoRecord(
            new Date(timestamp.getTime),
            latitude,
            longitude,
            Some(measurement),
            stateName,
            countyName
          )
        })
    }


    println(
      "- The min temperature per month from hottest month to coldest month:" +
        System.lineSeparator() +
        calculateMinTemperatureByMonth(records).mkString(System.lineSeparator()))

    println(
      "- The average temperature per month from hottest month to coldest month:" +
        System.lineSeparator() +
        calculateAvgTemperatureByMonth(records).mkString(System.lineSeparator()))

    println(
      "- The high temperature per month from hottest month to coldest month:" +
        System.lineSeparator() +
        calculateMaxTemperatureByMonth(records).mkString(System.lineSeparator()))


    println("- The min temperature per month from hottest month to coldest month for 'Michigan' state:" +
      System.lineSeparator() +
      calculateMinTemperatureByMonthForState(records, "Michigan").mkString(System.lineSeparator()))

    println("- The average temperature per month from hottest month to coldest month for 'Michigan' state:" +
      System.lineSeparator() +
      calculateAvgTemperatureByMonthForState(records, "Michigan").mkString(System.lineSeparator()))

    println("- The high temperature per month from hottest month to coldest month for 'Michigan' state:" +
      System.lineSeparator() +
      calculateMaxTemperatureByMonthForState(records, "Michigan").mkString(System.lineSeparator()))


    println("- The min temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      calculateMinTemperatureByMonthForCountry(records, "Baltimore").mkString(System.lineSeparator()))

    println("- The average temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      calculateAvgTemperatureByMonthForCountry(records, "Baltimore").mkString(System.lineSeparator()))

    println("- The high temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      calculateMaxTemperatureByMonthForCountry(records, "Baltimore").mkString(System.lineSeparator()))


    println("- findAllMaxTemperatureByMonthForCountry The high temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      findAllMaxTemperatureByMonthForCountry(records, "Baltimore").mkString(System.lineSeparator()))

    println("- findAllMaxTemperatureByMonthForState The high temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      findAllMaxTemperatureByMonthForState(records, "Baltimore").mkString(System.lineSeparator()))


    println("- How many days for all data were temperatures above 75ºF: " + hotDaysCount(records, 75))

    println("- Seq of all available countries grouped by state" +
      System.lineSeparator() +
      getSeqOfAllAvailableCounties(records).mkString(System.lineSeparator()))


    /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
      val avgMeasurementFilter = avg("measurement")
      groupTemperatureByMonthWithFilter(records, emptyFilter, avgMeasurementFilter)
    }

    /** Monthly high temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
      val maxMeasurementFilter = max("measurement")
      groupTemperatureByMonthWithFilter(records, emptyFilter, maxMeasurementFilter)
    }

    /** Monthly min temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyMeasurement] = {
      val minMeasurementFilter = min("measurement")
      groupTemperatureByMonthWithFilter(records, emptyFilter, minMeasurementFilter)
    }

    /** Monthly min temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param state state name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureByMonthForState(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = min("measurement")
      val filter = stateFilter(state)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Monthly average temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param state state name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureByMonthForState(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = avg("measurement")
      val filter = stateFilter(state)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param state state name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureByMonthForState(records: Dataset[MeteoRecord], state: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = max("measurement")
      val filter = stateFilter(state)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Monthly min temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureByMonthForCountry(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = max("measurement")
      val filter = countryFilter(country)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Monthly average temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureByMonthForCountry(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = avg("measurement")
      val filter = countryFilter(country)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureByMonthForCountry(records: Dataset[MeteoRecord], country: String): Seq[MonthlyMeasurement] = {
      val aggregateColumn = max("measurement")
      val filter = countryFilter(country)

      groupTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    // todo
    /**
     * Find all maximum monthly temperatures for the country.
     * If there were several days with a maximum temperature in a month, then get everything.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of meteo records ordered by temperatures from hottest to coldest.
     */
    def findAllMaxTemperatureByMonthForCountry(records: Dataset[MeteoRecord], country: String): Seq[MeteoRecord] = {
      val filter = countryFilter(country)
      val aggregateColumn = max("measurement")

      findAllTemperatureByMonthWithFilter(records, filter, aggregateColumn)
    }

    /** Find all maximum monthly temperatures for the state.
     * If there were several days with a maximum temperature in a month, then get everything.
     *
     * @param records meteo records Dataset
     * @param state state name String
     * @return sequence of meteo records ordered by temperatures from hottest to coldest
     */
    def findAllMaxTemperatureByMonthForState(records: Dataset[MeteoRecord], state: String): Seq[MeteoRecord] = {
      val filter = stateFilter(state)
      val aggregateColumn = max("measurement")

      findAllTemperatureByMonthWithFilter(records, filter, aggregateColumn)
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
      import spark.implicits._

      records
        .groupBy('stateName)
        .agg(collect_list("countyName") as "countyNames")
        .as[(String, Set[String])]
        .collect()
        .toMap
    }


    type MeteoRecordFilter = MeteoRecord => Boolean
    def emptyFilter: MeteoRecordFilter = _ => true
    def countryFilter: String => MeteoRecordFilter = countryName => record => record.countyName == countryName
    def stateFilter: String => MeteoRecordFilter = stateName => record => record.stateName == stateName

    def groupTemperatureByMonthWithFilter(
        records: Dataset[MeteoRecord],
        filter: MeteoRecordFilter,
        aggregateColumn: Column
      ): Seq[MonthlyMeasurement] = {

        import spark.implicits._

        records
          .filter(filter)
          .withColumn("month", month($"date"))
          .groupBy("month")
          .agg(aggregateColumn.as("measurementTempByMonth"))
          .map(x => {
            MonthlyMeasurement(x.getAs[Int]("month"), x.getAs[Double]("measurementTempByMonth"))
          })
          .orderBy(desc("measurement"))
          .collect()
    }

    def findAllTemperatureByMonthWithFilter(
        records: Dataset[MeteoRecord],
        filter: MeteoRecordFilter,
        aggregateColumn: Column
      ): Seq[MeteoRecord] = {

        import spark.implicits._

        val filteredRecords = records
          .filter(filter)

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

  }

}
