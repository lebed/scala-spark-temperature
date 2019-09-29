package temperature.test.spark

import temperature.test.model.{MeteoRecord, MonthlyAverage}

object Entry {

  def main(args: Array[String]) {

    /**
     * Dataset of temperature.csv
     */
    val records: AnyRef = ??? // Dataset[MeteoRecord]

    println(
      "Months from hottest to coldest:" +
        System.lineSeparator() +
        calculateAvgTemperatureByMonth(records).mkString(System.lineSeparator()))

    /** Monthly average temperature calculation, sorted in decreasing order of temperature.
     *
     * @param records meteo records Dataset
     * @return sequence of monthly average temperatures from hottest to coldest.
     */
    def calculateAvgTemperatureByMonth(records: AnyRef[MeteoRecord]): Seq[MonthlyAverage] = ???
  }
}
