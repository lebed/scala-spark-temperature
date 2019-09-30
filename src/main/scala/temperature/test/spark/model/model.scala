package temperature.test.spark

import java.sql.Date

package object model {

  case class MonthlyAverage(month: Int, avg: Double)

  case class MeteoRecord(
    date: Date,
    lat: Double,
    lon: Double,
    measurement: Option[Double],
    stateName: String,
    countyName: String
  )
}
