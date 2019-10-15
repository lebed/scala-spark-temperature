package temperature.test.spark

import java.sql.{Date, Timestamp}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import temperature.test.Messages
import temperature.test.spark.model.{MeteoRecord, MonthlyMeasurement}

object EntrySql {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Temperature Test")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    /**
     * DatFrame of temperature.csv
     */
    val records: DataFrame = {
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
          .toDF("date", "lat", "lon", "measurement", "stateName", "countryName")
    }

    records.createOrReplaceTempView("records")


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


    /** Monthly min temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureByMonth(records: DataFrame): Seq[MonthlyMeasurement] = {
      val aggregation = "min(measurement)"
      val where = ""
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
     *
     * @param records meteo records DataFrame
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureByMonth(records: DataFrame): Seq[MonthlyMeasurement] = {
      val aggregation = "avg(measurement)"
      val where = ""
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly high temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureByMonth(records: DataFrame): Seq[MonthlyMeasurement] = {
      val aggregation = "max(measurement)"
      val where = ""
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly min temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      val aggregation = "min(measurement)"
      val where = s"WHERE stateName = '$state'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly average temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      val aggregation = "avg(measurement)"
      val where = s"WHERE stateName = '$state'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      val aggregation = "max(measurement)"
      val where = s"WHERE stateName = '$state'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly min temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param country country name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      val aggregation = "min(measurement)"
      val where = s"WHERE countryName = '$country'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly average temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param country country name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      val aggregation = "avg(measurement)"
      val where = s"WHERE countryName = '$country'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      val aggregation = "max(measurement)"
      val where = s"WHERE countryName = '$country'"
      val query = buildQuery(aggregation, where)

      spark
        .sql(query)
        .as[MonthlyMeasurement]
        .collect()
    }

    /** Find all records with the maximum temperatures for every month.
     * If there were several days with a maximum temperature in a month, then display everything.
     *
     * @param records meteo records DataFrame
     * @return sequence of meteo records ordered by temperatures from hottest to coldest
     */
    def findAllRecordWithMaxTemperatureForEveryMonth(records: DataFrame): Seq[MeteoRecord] = {
      val query = "SELECT DISTINCT r1.date " +
        ", r1.measurement " +
        ", r1.lat " +
        ", r1.lon " +
        ", r1.stateName " +
        ", r1.countryName " +
        "FROM records AS r1 " +
        "INNER JOIN (SELECT month(date) month, max(measurement) measurement FROM records GROUP BY month(date)) AS r2 " +
        "ON r1.measurement = r2.measurement AND month(r1.date) = r2.month " +
        "ORDER BY r1.measurement DESC, r1.date"

      spark
        .sql(query)
        .as[MeteoRecord]
        .collect()
    }

    /** Counts how many days the temperature was higher for threshold for all data.
     *
     * @param records meteo records DataFrame
     * @param threshold threshold temperature
     * @return count of days with temperature above threshold
     */
    def hotDaysCount(records: DataFrame, threshold: Double): Long = {
      val query = s"SELECT count(*) AS count FROM records WHERE measurement > $threshold"

      spark
        .sql(query)
        .first
        .getAs[Long]("count")
    }

    /** Sequence of all available countries grouped by state.
     *
     * @param records meteo records DataFrame
     * @return map includes keys as a state name and values as country names
     */
    def getSeqOfAllAvailableCounties(records: DataFrame): Map[String, Set[String]] = {
      val query = s"SELECT stateName, collect_list(countryName) FROM records GROUP BY stateName"

      spark
        .sql(query)
        .as[(String, Set[String])]
        .collect()
        .toMap
    }

    def buildQuery(aggregation: String, where: String): String = {
      s"SELECT month(date) as month, $aggregation AS measurement FROM records $where GROUP BY month(date) ORDER BY measurement DESC"
    }

  }

}
