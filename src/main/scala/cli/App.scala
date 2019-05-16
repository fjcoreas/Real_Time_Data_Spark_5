package cli


import models.CityPopulationEntry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import services.CityPopulationProcessService

object App {

  def main(args: Array[String]): Unit = {
    //Init parallel context
    val sc: SparkContext = SparkContext.getOrCreate()

    //calling main app logic
    doRun(sc, args)

    //Stop parallel context
    sc.stop()
  }

  def doRun(sc: SparkContext, args: Array[String]): Unit ={
    val inputCityFemaleFilePath = args(0)
    val inputCityMaleFilePath = args(1)
    val outputFilePath = args(2)
    val operation = args(3);

    val cityFemaleLines: RDD[String] = sc.textFile(inputCityFemaleFilePath)
    val cityMaleLines: RDD[String] = sc.textFile(inputCityMaleFilePath)

    val cityFemale: RDD[CityPopulationEntry] = CityPopulationProcessService
      .buildCityPopulationEntryRDD(cityFemaleLines)

    val cityMale: RDD[CityPopulationEntry] = CityPopulationProcessService
      .buildCityPopulationEntryRDD(cityMaleLines)

    if("total_count_grouped_by_common_year_component" == operation) {
      val result = CityPopulationProcessService.totalCountByYear(cityFemale, cityMale)

      //deleting previous version of file
      Console.println("Deleting directory: " + outputFilePath)
      deleteFile(sc, outputFilePath)

      //Saving output
      Console.println("Saving results on: " +  outputFilePath)
      result.saveAsTextFile(outputFilePath)
    }

    if("distinct_city_female" == operation) {
      val result = CityPopulationProcessService.getDistinctCitiesOnFemaleCollection(cityFemale)

      //deleting previous version of file
      Console.println("Deleting directory: " + outputFilePath)
      deleteFile(sc, outputFilePath)

      //Saving output
      Console.println("Saving results on: " +  outputFilePath)
      result.saveAsTextFile(outputFilePath)
    }
  }


  def deleteFile(sc: SparkContext, path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.delete(new org.apache.hadoop.fs.Path(path), true)
  }
}
