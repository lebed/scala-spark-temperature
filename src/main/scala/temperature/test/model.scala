package temperature.test

import java.time.LocalDate

package object model {

  case class MonthlyAverage(month: Int, avg: Double)

  case class MeteoRecord(
    date: LocalDate,
    lat: Double,
    lon: Double,
    measurement: Option[Double],
    stateName: String,
    countyName: String
  )

}
