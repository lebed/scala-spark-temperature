package temperature.test.scala

import java.sql.{Date, Timestamp}
import java.time.LocalDate

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSpec
import temperature.test.spark.Entry
import temperature.test.spark.model.{MeteoRecord, MonthlyMeasurement}

class EntrySuite extends FunSpec {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("myApp")
    .getOrCreate()

  import spark.implicits._

  val records: Dataset[MeteoRecord] = {
    spark
      .read
      .options(Map(
        "header" -> "true",
        "inferSchema" -> "true"
      ))
      .csv("src/test/resources/test-data.csv")
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


  it("calculateMinTemperatureByMonth") {
    val expectedResult = Array(
      MonthlyMeasurement(7, 58),
      MonthlyMeasurement(6, 54.0),
      MonthlyMeasurement(4, 33),
    )
    val actualResult = Entry.calculateMinTemperatureByMonth(records)

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateAvgTemperatureByMonth") {
    val expectedResult = Array(
      MonthlyMeasurement(7, 58),
      MonthlyMeasurement(6, 55.5),
      MonthlyMeasurement(4, 37),
    )
    val actualResult = Entry.calculateAvgTemperatureByMonth(records)

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateMaxTemperatureByMonth") {
    val expectedResult = Array(
      MonthlyMeasurement(7, 58),
      MonthlyMeasurement(6, 55.5),
      MonthlyMeasurement(4, 37.0),
    )
    val actualResult = Entry.calculateAvgTemperatureByMonth(records)

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateMinTemperatureForStateByMonth") {
    val stateName = "Connecticut"
    val expectedResult = Array(
      MonthlyMeasurement(7,58.0),
      MonthlyMeasurement(6,54.0),
    )
    val actualResult = Entry.calculateMinTemperatureForStateByMonth(records, stateName)

    assert(actualResult.size == 2)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateAvgTemperatureForStateByMonth") {
    val stateName = "Connecticut"
    val expectedResult = Array(
      MonthlyMeasurement(7,58.0)
    )
    val actualResult = Entry.calculateAvgTemperatureForStateByMonth(records, stateName)

    assert(actualResult.size == 2)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateMaxTemperatureForStateByMonth") {
    val stateName = "Connecticut"
    val expectedResult = Array(
      MonthlyMeasurement(7,58.0)
    )
    val actualResult = Entry.calculateMaxTemperatureForStateByMonth(records, stateName)

    assert(actualResult.size == 2)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("findAllRecordWithMaxTemperatureForEveryMonth") {
    val expectedResult = Array(
      MeteoRecord(Date.valueOf(LocalDate.of(1993, 7, 1)), 41.003611, -73.585,
        Some(58.0), "Connecticut", "Fairfield"),
      MeteoRecord(Date.valueOf(LocalDate.of(1993, 6, 4)), 41.003611, -73.585,
        Some(57.0), "Connecticut", "Fairfield")
    )
    val actualResult = Entry.findAllRecordWithMaxTemperatureForEveryMonth(records)

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  describe("hotDaysCount") {
    it("hot days above 50") {
      val threshold = 50
      val expectedResult = 3
      val actualResult = Entry.hotDaysCount(records, threshold)

      assert(actualResult == expectedResult)
    }

    it("hot days above 40") {
      val threshold = 40
      val expectedResult = 4
      val actualResult = Entry.hotDaysCount(records, threshold)

      assert(actualResult == expectedResult)
    }
  }

  it("getSeqOfAllAvailableCounties") {
    val expectedResult = Map(
      "Colorado" -> Set("Arapahoe"),
      "Connecticut" -> Set("Fairfield"),
      "Maryland" -> Set("Prince George's")
    )

    val actualResult = Entry.getSeqOfAllAvailableCounties(records)

    assert(actualResult.size == 3)
    expectedResult.foreach(i => {
      assert(actualResult(i._1) == expectedResult(i._1))
    })
  }

}
