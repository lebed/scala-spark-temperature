package temperature.test.spark

import java.sql.Date
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import temperature.test.spark.model.{MeteoRecord, MonthlyAverage}

object Entry {

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
          "timestampFormat" -> "yyyy-MM-dd",
          "inferSchema" -> "true"
        ))
        .csv("src/main/resources/temperature.csv")
        .map(field => {
          // todo should refactor this part of code, maybe possible to immediately map to the model
          import java.text.SimpleDateFormat
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val date = format.parse(field(11).toString)
          val latitude = field(5).toString
          val longitude = field(6).toString
          val measurement = field(13).asInstanceOf[Double]
          val stateName = field(21).toString
          val countyName = field(22).toString

            MeteoRecord(
              new Date(date.getTime),
              latitude.toDouble,
              longitude.toDouble,
              Some(measurement),
              stateName,
              countyName
            )

        })
        .as[MeteoRecord]
    }

    println(
      "- The high temperature per month from hottest month to coldest month:" +
        System.lineSeparator() +
        calculateAvgTemperatureByMonth(records).mkString(System.lineSeparator()))

    println(
      "- The high temperature per month from hottest month to coldest month for 'Michigan' state:" +
        System.lineSeparator() +
        calculateMaxTemperatureByMonth(records).mkString(System.lineSeparator()))

    println("- The high temperature per month from hottest month to coldest month for 'Michigan' state:" +
      System.lineSeparator() +
      calculateMaxTemperatureByMonthForState(records, "Michigan").mkString(System.lineSeparator()))

    println("- The high temperature per month from hottest month to coldest month for 'Baltimore' country:" +
      System.lineSeparator() +
      calculateMaxTemperatureByMonthForCountry(records, "Baltimore").mkString(System.lineSeparator()))

    println("- How many days were temperatures above 75ºF: " + hotDaysCount(records, 75))

    println("- Seq of all available countries grouped by state" +
      System.lineSeparator() +
      getSeqOfAllAvailableCounties(records).mkString(System.lineSeparator()))

    /** Monthly average temperature calculation, sorted in decreasing order of avg measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly average temperatures from hottest to coldest.
     */
    def calculateAvgTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MonthlyAverage] = {
      import spark.implicits._

      records
        .withColumn("month", month($"date"))
        .groupBy("month")
        .agg(avg("measurement").as("measurementTempByMonth"))
        .map(x => {
          MonthlyAverage(x.getAs[Int]("month"), x.getAs[Double]("measurementTempByMonth"))
        })
        .orderBy(desc("avg"))
        .collect()
        .toSeq
    }

    /** Monthly high temperature calculation, sorted in decreasing order of measurement.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly high temperatures from hottest to coldest.
     */
    def calculateMaxTemperatureByMonth(records: Dataset[MeteoRecord]): Seq[MeteoRecord] = {
      import spark.implicits._

      val groupedRecordsByMonth = records
          .withColumn("month", month($"date"))
          .groupBy('month)
          .agg(max('measurement) as "maxMeasurement")

      records
        .join(groupedRecordsByMonth,
          groupedRecordsByMonth.col("maxMeasurement") === records.col("measurement") &&
            groupedRecordsByMonth.col("month") === month(records.col("date"))
        ).dropDuplicates()
        .drop("maxMeasurement", "month")
        .orderBy(desc("measurement"))
        .as[MeteoRecord]
        .collect()
        .toSeq
    }

    /** Monthly high temperature calculation for state, sorted in decreasing order of measurement.
     *
     * @param records iterator of MeteoRecord
     * @return sequence of monthly high temperatures from hottest to coldest.
     */
    def calculateMaxTemperatureByMonthForState(records: Dataset[MeteoRecord], state: String): Seq[MeteoRecord] = {
      import spark.implicits._

      val groupedRecordsByMonth = records
        .filter(_.stateName == state)
        .withColumn("month", month($"date"))
        .groupBy('month)
        .agg(max('measurement) as "maxMeasurement")

      records
        .filter(_.stateName == state)
        .join(groupedRecordsByMonth,
          groupedRecordsByMonth.col("maxMeasurement") === records.col("measurement") &&
            groupedRecordsByMonth.col("month") === month(records.col("date"))
        )
        .dropDuplicates()
        .drop("maxMeasurement", "month")
        .orderBy(desc("measurement"))
        .as[MeteoRecord]
        .collect()
    }

    /** Monthly high temperature calculation for country, sorted in decreasing order of measurement.
     *
     * @param records iterator of MeteoRecord
     * @return sequence of monthly high temperatures from hottest to coldest.
     */
    def calculateMaxTemperatureByMonthForCountry(records: Dataset[MeteoRecord], country: String): Seq[MeteoRecord] = {
      import spark.implicits._

      val groupedRecordsByMonth = records
        .filter(_.stateName == country)
        .withColumn("month", month($"date"))
        .groupBy('month)
        .agg(max('measurement) as "maxMeasurement")

      records
        .filter(_.stateName == country)
        .join(groupedRecordsByMonth,
          groupedRecordsByMonth.col("maxMeasurement") === records.col("measurement") &&
            groupedRecordsByMonth.col("month") === month(records.col("date"))
        )
        .dropDuplicates()
        .drop("maxMeasurement", "month")
        .orderBy(desc("measurement"))
        .as[MeteoRecord]
        .collect()
    }

    /** Counts how many days the temperature was higher for threshold for all data
     *
     * @param records iterator of MeteoRecord
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
     * @param records iterator of MeteoRecord
     * @return sequence of monthly high temperatures from hottest to coldest.
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

  }
}
