package temperature.test.scala

import temperature.test.scala.model.MeteoRecord

object RecordService {
  def groupRecordsByMonth(records: Iterator[MeteoRecord]): Map[Int, Seq[MeteoRecord]] = {
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

  def countMaxTemperature(groupedRecords: Map[Int, Seq[MeteoRecord]]): Seq[MeteoRecord] = {
    groupedRecords.values.flatMap(records => {
      val filteredRecords = records.filter(x => x.measurement.isDefined)
      val maxMeasurement = filteredRecords.maxBy(_.measurement.get)
      records.filter(_.measurement.get == maxMeasurement.measurement.get).toSet
    }).toSeq
  }
}
