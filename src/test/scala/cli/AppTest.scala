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

}
