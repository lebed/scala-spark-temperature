package temperature.test.spark

import java.sql.Date

package object model {

  case class MonthlyMeasurement(month: Int, measurement: Double)

  case class MeteoRecord(
    date: Date,
    lat: Double,
    lon: Double,
    measurement: Option[Double],
    stateName: String,
    countyName: String
  )
}
