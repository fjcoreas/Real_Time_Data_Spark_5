package cli

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class AppTest extends FunSuite with SharedSparkContext with BeforeAndAfterEach{

  def fileExist(path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new org.apache.hadoop.fs.Path(path + "/_SUCCESS"))
  }

  test("Should load female a male files and produce a " +
    "new file with Total count of females and male grouped by common year component") {

    val femalePopulationPath:String = "./data/city_female_population"
    val malePopulationPath: String = "./data/city_male_population"
    val outpath: String = "./tmp/total_count_grouped_by_common_year_component"

    //Calling App.doRun instead of main to pass test spark context
    App.doRun(sc, Array(
      femalePopulationPath, malePopulationPath,
      outpath, "total_count_grouped_by_common_year_component"
    ))

    assert(fileExist("./tmp/total_count_grouped_by_common_year_component"))
  }

  test("Should load female a male files and produce a " +
    "Add the functionality to get the distinct cities on female collection") {

    val femalePopulationPath:String = "./data/city_female_population"
    val malePopulationPath: String = "./data/city_male_population"
    val outpath: String = "./tmp/distinct_city_female"

    //Calling App.doRun instead of main to pass test spark context
    App.doRun(sc, Array(
      femalePopulationPath, malePopulationPath,
      outpath, "distinct_city_female"
    ))

    assert(fileExist("./tmp/distinct_city_female"))
  }

  test("Should load female a male files and produce a " +
    "Add the functionality to get the distinct cities on both collections") {

    val femalePopulationPath:String = "./data/city_female_population"
    val malePopulationPath: String = "./data/city_male_population"
    val outpath: String = "./tmp/distinct_city_all"

    //Calling App.doRun instead of main to pass test spark context
    App.doRun(sc, Array(
      femalePopulationPath, malePopulationPath,
      outpath, "distinct_city_all"
    ))

    assert(fileExist("./tmp/distinct_city_all"))
  }

  test("Should load female a male files and produce a " +
    "Add the functionality to get total count of people, male or female by year") {

    val femalePopulationPath:String = "./data/city_female_population"
    val malePopulationPath: String = "./data/city_male_population"
    val outpath: String = "./tmp/total_count_by_year"

    //Calling App.doRun instead of main to pass test spark context
    App.doRun(sc, Array(
      femalePopulationPath, malePopulationPath,
      outpath, "total_count_by_year"
    ))

    assert(fileExist("./tmp/total_count_by_year"))
  }

  test("Should load female a male files and produce a " +
    "Add the functionality to get the total count of people that live on Urban Agglomeration by year") {

    val femalePopulationPath:String = "./data/city_female_population"
    val malePopulationPath: String = "./data/city_male_population"
    val outpath: String = "./tmp/urban_agglomeration_by_year"

    //Calling App.doRun instead of main to pass test spark context
    App.doRun(sc, Array(
      femalePopulationPath, malePopulationPath,
      outpath, "urban_agglomeration_by_year"
    ))

    assert(fileExist("./tmp/urban_agglomeration_by_year"))
  }

}
