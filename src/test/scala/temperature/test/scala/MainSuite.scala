package temperature.test.scala

import java.time.LocalDate

import org.scalatest.FunSpec
import temperature.test.model.{MeteoRecord}
import Main._
import com.github.tototoshi.csv.CSVReader
import scala.io.Source

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
        val actualResult = toRecord(source.toSeq(5))
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

}