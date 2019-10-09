package temperature.test.scala

import java.time.LocalDate

package object model {

  case class MonthlyMeasurement(month: Int, measurement: Double)

  case class MeteoRecord(
    date: LocalDate,
    lat: Double,
    lon: Double,
    measurement: Option[Double],
    stateName: String,
    countyName: String
  )

}
