package services

import models.{CityPopulationEntry, CityPopulationEntryBuilder}
import org.apache.spark.rdd.RDD

object CityPopulationProcessService {

  def totalCountByYear(rddMales: RDD[CityPopulationEntry], rddFemales: RDD[CityPopulationEntry])
    : RDD[(Int, Double)] = {
    val mapMale: RDD[(Int, Double)] = rddMales.map(entry=>(entry.year, entry.value)).reduceByKey(_+_)
    val mapFemales: RDD[(Int, Double)] = rddFemales.map(entry=>(entry.year, entry.value)).reduceByKey(_+_)

    mapMale.join(mapFemales).map(jentry=>(jentry._1, jentry._2._1 + jentry._2._2))
  }

  def getDistinctCitiesOnFemaleCollection(rddFemales: RDD[CityPopulationEntry])
  : RDD[String] = {
      rddFemales.map(o=> o.city).distinct()
  }

  def getDistinctCitiesOnAllCollection(rddMales: RDD[CityPopulationEntry], rddFemales: RDD[CityPopulationEntry])
    : RDD[String] = {
    val allcities = rddMales.union(rddFemales)
    allcities.map(o=> o.city).distinct()
  }

  def totalCountYear(rddMales: RDD[CityPopulationEntry], rddFemales: RDD[CityPopulationEntry])
    : RDD[(Int, Double)] = {
    rddMales.union(rddFemales).map(entry=>(entry.year, entry.value)).reduceByKey(_+_)
  }

  def CountOfPeopleThatLiveonUrbanAgglomerationbyyear (rddMales: RDD[CityPopulationEntry], rddFemales: RDD[CityPopulationEntry])
  : RDD[(Int,Double)] = {
    val union_all: RDD[(String, Int, Double)] = rddMales.union(rddFemales).map(entry =>(entry.cityType,entry.year,entry.value))
    val filterurban: RDD[(Int, Double)] = union_all.filter(entry=>entry._1.contains("Urban agglomeration")).map(entry =>(entry._2, entry._3))
    filterurban.reduceByKey(_+_)
  }

  def buildCityPopulationEntryRDD(lines: RDD[String]): RDD[CityPopulationEntry] = {
    lines.map(line =>
      CityPopulationEntryBuilder.buildEntryFromLine(line)
    )
  }

}
