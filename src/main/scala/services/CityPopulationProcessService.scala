package services

import models.{CityPopulationEntry, CityPopulationEntryBuilder}
import org.apache.spark.rdd.RDD

object CityPopulationProcessService {
  def buildCityPopulationEntryRDD(mockLines: RDD[String]): RDD[CityPopulationEntry] = {
    mockLines.map(line =>
      CityPopulationEntryBuilder.buildEntryFromLine(line)
    )
  }

}
