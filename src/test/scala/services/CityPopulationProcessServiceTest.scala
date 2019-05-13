package services

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import models.{CityPopulationEntry, CityPopulationEntryBuilder}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class CityPopulationProcessServiceTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def buildMockCityPopulationEntryRDD(lines: Array[String]) = {
      val array: Array[CityPopulationEntry] = lines.map(
        l=> CityPopulationEntryBuilder.buildEntryFromLine(l)
      )
      sc.parallelize(array)
  }

  def buildMockStringRDD(lines: Array[String]): RDD[String] ={
    sc.parallelize(lines);
  }

  test("Transform RDD of String into RDD to CityPopulation "){
    val mockLines: Array[String] = Array[String](
      "\"Kazakhstan\",2009,\"Total\",\"Male\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2009,74261,\n",
      "\"Kazakhstan\",2008,\"Total\",\"Male\",\"Karaganda\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2008,208286,\n",
      "\"Jordan\",2004,\"Total\",\"Male\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n"
    )
    val expectedOutput = buildMockCityPopulationEntryRDD(mockLines)
    val realOuput: RDD[CityPopulationEntry] = CityPopulationProcessService.buildCityPopulationEntryRDD(buildMockStringRDD(mockLines));

    assertRDDEquals(expectedOutput, realOuput)
  }

}
