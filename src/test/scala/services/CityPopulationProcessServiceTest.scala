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

    val realInput = buildMockStringRDD(mockLines);
    val expectedOutput = buildMockCityPopulationEntryRDD(mockLines)

    val realOuput: RDD[CityPopulationEntry] = CityPopulationProcessService.buildCityPopulationEntryRDD(
      realInput
    );

    assertRDDEquals(expectedOutput, realOuput)
  }

  test("Total count of females and male grouped by common year component"){
    val mockMales: Array[String] = Array[String](
      "\"Kazakhstan\",2009,\"Total\",\"Male\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2009,74261,\n",
      "\"Kazakhstan\",2008,\"Total\",\"Male\",\"Karaganda\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2008,208286,\n",
      "\"Jordan\",2004,\"Total\",\"Male\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n"
    )

    val mockFemales: Array[String] = Array[String](
      "\"Kazakhstan\",2003,\"Total\",\"Female\",\"Rudni\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,55483,\n",
      "\"Kazakhstan\",2004,\"Total\",\"Female\",\"Rudni\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,56378,\n"
    )

    val expectedOutput: RDD[(Int, Double)] = sc.parallelize(Array(
      (2004, 259008.0)
    ))

    val rddMales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockMales)
    val rddFemales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockFemales)

    val realOutput: RDD[(Int, Double)] = CityPopulationProcessService.totalCountByYear(rddMales, rddFemales);

    assertRDDEquals(expectedOutput, realOutput)
  }

  test("Add the functionality to get the distinct cities on female collection"){
    val mockFemales: Array[String] = Array[String](
      "\"Kazakhstan\",2009,\"Total\",\"Female\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2009,74261,\n",
      "\"Kazakhstan\",2008,\"Total\",\"Female\",\"Karaganda\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2008,208286,\n",
      "\"Jordan\",2004,\"Total\",\"Female\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n",
      "\"Jordan\",2010,\"Total\",\"Female\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n")

    val expectedOutput: RDD[String] = sc.parallelize(Array(
      "Karaganda",
      "Zarqa",
      "Aktau"
    ))

    val rddFemales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockFemales)

    val realOutput: RDD[String] = CityPopulationProcessService.getDistinctCitiesOnFemaleCollection(rddFemales);

    assertRDDEquals(expectedOutput, realOutput)
  }

  test("Add the functionality to get the distinct cities on both collections"){
    val mockMales: Array[String] = Array[String](
      "\"Kazakhstan\",2009,\"Total\",\"Male\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2009,74261,\n",
      "\"Kazakhstan\",2008,\"Total\",\"Male\",\"Karaganda\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2008,208286,\n",
      "\"Jordan\",2004,\"Total\",\"Male\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n"
    )
    val mockFemales: Array[String] = Array[String](
      "\"Kazakhstan\",2003,\"Total\",\"Female\",\"Rudni\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,55483,\n",
      "\"Kazakhstan\",2004,\"Total\",\"Female\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,56378,\n"
    )

    val expectedOutput: RDD[String] = sc.parallelize(Array("Aktau","Karaganda","Zarqa","Rudni"))

    val rddFemales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockFemales)
    val rddMales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockMales)

    val realOutput: RDD[String] = CityPopulationProcessService.getDistinctCitiesOnAllCollection(rddMales, rddFemales);

    assertRDDEquals(expectedOutput, realOutput)
  }

  test("Add the functionality to get total count of people, male or female by year") {
    val mockMales: Array[String] = Array[String](
      "\"Jordan\",2004,\"Total\",\"Male\",\"Zarqa\",\"City proper\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n"
    )
    val mockFemales: Array[String] = Array[String](
      "\"Kazakhstan\",2003,\"Total\",\"Female\",\"Rudni\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,55483,\n",
      "\"Kazakhstan\",2004,\"Total\",\"Female\",\"Aktau\",\"City proper\",\"Estimate - de facto\",\"Final figure, complete\",2004,56378,\n"
    )

    val expectedOutput: RDD[(Int, Double)] = sc.parallelize(Array(
      (2004, 259008.0), (2003, 55483.0)
    ))

    val rddFemales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockFemales)
    val rddMales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockMales)

    val realOutput: RDD[(Int, Double)] = CityPopulationProcessService.totalCountYear(rddMales, rddFemales);

    assertRDDEquals(expectedOutput, realOutput)
  }

  test("Add the functionality to get the total count of people that live on Urban Agglomeration by year") {
    val mockMales: Array[String] = Array[String](
      "\"Jordan\",2004,\"Total\",\"Male\",\"Zarqa\",\"Urban agglomeration\",\"Census - de facto - complete tabulation\",\"Final figure, complete\",2007,202630,77\n"
    )
    val mockFemales: Array[String] = Array[String](
      "\"Kazakhstan\",2004,\"Total\",\"Female\",\"Rudni\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2004,55483,\n",
      "\"Kazakhstan\",2004,\"Total\",\"Female\",\"Aktau\",\"Urban agglomeration\",\"Estimate - de facto\",\"Final figure, complete\",2004,56378,\n"
    )

    val expectedOutput: RDD[(Int,Double)] = sc.parallelize(Array(
      (2004,314491.0)
    ))

    val rddFemales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockFemales)
    val rddMales: RDD[CityPopulationEntry] = buildMockCityPopulationEntryRDD(mockMales)

    val realOutput: RDD[(Int,Double)] = CityPopulationProcessService.CountOfPeopleThatLiveonUrbanAgglomerationbyyear(rddMales, rddFemales);

    assertRDDEquals(expectedOutput, realOutput)
  }




}