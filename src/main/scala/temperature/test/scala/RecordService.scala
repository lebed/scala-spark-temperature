package temperature.test.scala

import temperature.test.scala.model.{MeteoRecord, MonthlyMeasurement}

import scala.util.Try

object RecordService {

  type MeteoRecordFilter = MeteoRecord => Boolean
  def emptyFilter: MeteoRecordFilter = _ => true
  def countryFilter: String => MeteoRecordFilter = countryName => record => record.countyName == countryName
  def stateFilter: String => MeteoRecordFilter = stateName => record => record.stateName == stateName

  def calculateTemperatureWithFilterByMonth(
      records: Iterator[MeteoRecord],
      f: MeteoRecord => Boolean,
      aggregate: Seq[Double] => Double
    ): Seq[MonthlyMeasurement] = {

    val countTemperatureWithFilter: Map[Int, Seq[MeteoRecord]] => Seq[MonthlyMeasurement] = groupedRecords => {
      groupedRecords
        .map {
          case (month, records) => MonthlyMeasurement(month, aggregate(records.flatMap(_.measurement)))
        }
        .toSeq
        .sortBy(_.measurement)(Ordering[Double].reverse)
    }

    val filteredRecords = records.filter(f)
    (groupRecordsByMonth andThen countTemperatureWithFilter)(filteredRecords)
  }

  def calculateMaxTemperatureWithFilterByMonth(
      records: Iterator[MeteoRecord],
      aggregate: Seq[Double] => Double
    ): Seq[MeteoRecord] = {

    val countMaxTemperature: Map[Int, Seq[MeteoRecord]] => Seq[MeteoRecord] = groupedRecords => {
      groupedRecords.values.flatMap(records => {
        val maxMeasurement = aggregate(records.flatMap(_.measurement))

        // it's faster than records.filter(_.measurement == Some(maxMeasurement)).toSet
        records.foldLeft(Set.empty[MeteoRecord])((acc, curr) => {
          curr.measurement match {
            case Some(measurement) if measurement == maxMeasurement => acc + curr
            case _ => acc
          }
        })
      })
        .toSeq
        .sortBy(_.measurement.get)(Ordering[Double].reverse)
    }

    (groupRecordsByMonth andThen countMaxTemperature)(records)
  }

  val groupRecordsByMonth: Iterator[MeteoRecord] => Map[Int, Seq[MeteoRecord]] = records => {
    // that's why I don't use cast to Seq and group by for iterator
    // https://stackoverflow.com/questions/52789027/scala-iterator-vs-other-collections
    records.foldLeft(Map.empty[Int, Seq[MeteoRecord]])((acc, curr) =>
      curr.measurement match {
        case Some(_) =>
          acc + (curr.date.getMonthValue -> (acc.getOrElse(curr.date.getMonthValue, Seq.empty[MeteoRecord]) :+ curr))
        case None => acc
      }
    )
  }

  def extractDouble(x: String): Option[Double] = Try(x.toDouble).toOption
}
