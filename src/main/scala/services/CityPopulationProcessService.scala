package services

import models.{CityPopulationEntry, CityPopulationEntryBuilder}
import org.apache.spark.rdd.RDD

object CityPopulationProcessService {
  def totalCountByYear(rddMales: RDD[CityPopulationEntry], rddFemales: RDD[CityPopulationEntry])
    : RDD[(Int, Double)] = {
    val mapMale: RDD[(Int, Double)] = rddMales.map(entry=>(entry.year, entry.value)).reduceByKey(_+_)
    val mapFemales: RDD[(Int, Double)] = rddFemales.map(entry=>(entry.year, entry.value)).reduceByKey(_+_)

    mapMale.join(mapFemales).map(jentry=>(jentry._1, jentry._2._1 + jentry._2._2)).reduceByKey(_+_)
  }

  def buildCityPopulationEntryRDD(lines: RDD[String]): RDD[CityPopulationEntry] = {
    lines.map(line =>
      CityPopulationEntryBuilder.buildEntryFromLine(line)
    )
  }

}
