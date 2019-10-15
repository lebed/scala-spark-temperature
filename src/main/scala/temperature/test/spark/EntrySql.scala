package temperature.test.spark

import java.sql.{Date, Timestamp}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, _}
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
          .toDF("date", "lat", "lon", "measurement", "stateName", "countryName")
    }

    records.createOrReplaceTempView("records")


    println(Messages.MIN_TEMPERATURE_BY_MONTH + System.lineSeparator() +
      calculateMinTemperatureByMonth(records).mkString(System.lineSeparator()))


    /** Monthly min temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureByMonth(records: DataFrame): Seq[MonthlyMeasurement] = {
      // val minMeasurementFilter = min("measurement")
      // groupTemperatureWithFilterByMonth(records, emptyFilter, minMeasurementFilter)

      val query = "SELECT month(date) as month, min(measurement) AS measurement FROM records GROUP BY month(date) ORDER BY measurement DESC"

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
      ???
    }

    /** Monthly high temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureByMonth(records: DataFrame): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly min temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly average temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly min temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param country country name String
     * @return sequence of monthly min temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMinTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly average temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records DataFrame
     * @param country country name String
     * @return sequence of monthly average temperatures ordered by measurement from hottest to coldest.
     */
    def calculateAvgTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @param country country name String
     * @return sequence of monthly high temperatures ordered by measurement from hottest to coldest.
     */
    def calculateMaxTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MonthlyMeasurement] = {
      ???
    }

    /** Find all maximum monthly temperatures for the state.
     * If there were several days with a maximum temperature in a month, then get everything.
     *
     * @param records meteo records DataFrame
     * @param state state name String
     * @return sequence of meteo records ordered by temperatures from hottest to coldest
     */
    def findAllMaxTemperatureForStateByMonth(records: DataFrame, state: String): Seq[MeteoRecord] = {
      ???
    }

    /**
     * Find all maximum monthly temperatures for the country.
     * If there were several days with a maximum temperature in a month, then get everything.
     *
     * @param records meteo records DataFrame
     * @param country country name String
     * @return sequence of meteo records ordered by temperatures from hottest to coldest.
     */
    def findAllMaxTemperatureForCountryByMonth(records: DataFrame, country: String): Seq[MeteoRecord] = {
      ???
    }

    /** Counts how many days the temperature was higher for threshold for all data.
     *
     * @param records meteo records DataFrame
     * @param threshold threshold temperature
     * @return count of days with temperature above threshold
     */
    def hotDaysCount(records: DataFrame, threshold: Double): Long = {
      ???
    }

    /** Sequence of all available countries grouped by state.
     *
     * @param records meteo records DataFrame
     * @return map includes keys as a state name and values as country names
     */
    def getSeqOfAllAvailableCounties(records: DataFrame): Map[String, Set[String]] = {
      ???
    }
  }

}
