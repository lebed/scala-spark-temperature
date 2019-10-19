package temperature.test.scala

import scala.io.Source
import java.time.LocalDate
import org.scalatest.FunSpec
import com.github.tototoshi.csv.CSVReader
import temperature.test.scala.model.{MeteoRecord, MonthlyMeasurement}
import Main._

class MainSuite extends FunSpec {

  def withRecordsIterator[A, B](f: Iterator[MeteoRecord] => A): A = {
    initResources() { source =>
      f(source.flatMap(toRecord))
    }
  }

  def initResources[A]()(f: Iterator[Seq[String]] => A): A = {
    val resource = CSVReader.open(Source.fromInputStream(getClass().getClassLoader.getResourceAsStream("test-data.csv")))
    try {
      f(resource.iterator)
    } finally {
      resource.close()
    }
  }

  describe("toRecord") {
    it("Header record") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq.head)
        val expectedResult = None

        assert(actualResult == expectedResult)
      }
    }

    it("Record 1") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq(1))
        val expectedResult = Some(MeteoRecord(
          LocalDate.of(1993, 4, 13),
          39.657222,
          -104.998056,
          Some(33.0),
          "Colorado",
          "Arapahoe")
        )

        assert(actualResult == expectedResult)
      }
    }

    it("Record 2") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq(2))
        val expectedResult = Some(MeteoRecord(
          LocalDate.of(1993, 4, 14),
          39.657222,
          -104.998056,
          Some(33.0),
          "Colorado",
          "Arapahoe")
        )

        assert(actualResult == expectedResult)
      }
    }

    it("Record 3") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq(3))
        val expectedResult = Some(MeteoRecord(
          LocalDate.of(1993, 6, 4),
          41.003611,
          -73.585,
          Some(57.0),
          "Connecticut",
          "Fairfield")
        )

        assert(actualResult == expectedResult)
      }
    }

    it("Record 4") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq(4))
        val expectedResult = Some(MeteoRecord(
          LocalDate.of(1993, 6, 5),
          41.003611,
          -73.585,
          Some(54.0),
          "Connecticut",
          "Fairfield")
        )

        assert(actualResult == expectedResult)
      }
    }

    it("Record 5") {
      initResources() { source =>
        val actualResult = toRecord(source.toSeq(6))
        val expectedResult = Some(MeteoRecord(
          LocalDate.of(1993, 4, 27),
          38.851667,
          -76.932778,
          Some(45.0),
          "Maryland",
          "Prince George's")
        )

        assert(actualResult == expectedResult)
      }
    }
  }

  it("calculateMinTemperatureByMonth") {
    val expectedResult = Array(
      MonthlyMeasurement(7, 58),
      MonthlyMeasurement(6, 54.0),
      MonthlyMeasurement(4, 33),
    )
    val actualResult = withRecordsIterator(calculateMinTemperatureByMonth)

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
    val actualResult = withRecordsIterator(calculateAvgTemperatureByMonth)

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("calculateMaxTemperatureByMonth") {
    val expectedResult = Array(
      MonthlyMeasurement(7, 58),
      MonthlyMeasurement(6, 57.0),
      MonthlyMeasurement(4, 45.0),
    )
    val actualResult = withRecordsIterator(calculateMaxTemperatureByMonth)

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
    val actualResult = withRecordsIterator(ri => calculateMinTemperatureForStateByMonth(ri, stateName))

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
    val actualResult = withRecordsIterator(ri => calculateAvgTemperatureForStateByMonth(ri, stateName))

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
    val actualResult = withRecordsIterator(ri => calculateMaxTemperatureForStateByMonth(ri, stateName))

    assert(actualResult.size == 2)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  it("findAllRecordWithMaxTemperatureForEveryMonth") {
    val expectedResult = Array(
      MeteoRecord(LocalDate.of(1993, 7, 1), 41.003611, -73.585,
        Some(58.0), "Connecticut", "Fairfield"),
      MeteoRecord(LocalDate.of(1993, 6, 4), 41.003611, -73.585,
        Some(57.0), "Connecticut", "Fairfield")
    )
    val actualResult = withRecordsIterator(ri => findAllRecordWithMaxTemperatureForEveryMonth(ri))

    assert(actualResult.size == 3)
    expectedResult.indices.foreach(i => {
      assert(actualResult(i) == expectedResult(i))
    })
  }

  describe("hotDaysCount") {
    it("hot days above 50") {
      val threshold = 50
      val expectedResult = 3
      val actualResult = withRecordsIterator(ri => hotDaysCount(ri, threshold))

      assert(actualResult == expectedResult)
    }

    it("hot days above 40") {
      val threshold = 40
      val expectedResult = 4
      val actualResult = withRecordsIterator(ri => hotDaysCount(ri, threshold))

      assert(actualResult == expectedResult)
    }
  }

  it("getSeqOfAllAvailableCounties") {
    val expectedResult = Map(
      "Colorado" -> Set("Arapahoe"),
      "Connecticut" -> Set("Fairfield"),
      "Maryland" -> Set("Prince George's")
    )

    val actualResult = withRecordsIterator(getSeqOfAllAvailableCounties)

    assert(actualResult.size == 3)
    expectedResult.foreach(i => {
      assert(actualResult(i._1) == expectedResult(i._1))
    })
  }

}