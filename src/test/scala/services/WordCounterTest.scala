package services

import java.io.File

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class WordCounterTest extends FunSuite with BeforeAndAfterEach with SharedSparkContext with RDDComparisons {
  val wordCountService: WordCounter = new WordCounterImpl()

  override def beforeEach() = {
    try {
      FileUtils.deleteDirectory(new File("./tmp/mocks"))
    } catch {
      case e: Exception => {}
    }

    FileUtils.forceMkdir(new File("./tmp/mocks"))
    FileUtils.copyDirectoryToDirectory(new File("./src/test/mocks"), new File("./tmp/"))
  }

  override  def afterEach() = {
    FileUtils.deleteDirectory(new File("./tmp/mocks"))
  }

  //Testing WordCount:wordCount using fake collection
  test("test static method wordCount of object WordCount using fake collection") {
    // Defining expected result as list of (key, value)
    val expectedResult = List(("a", 3),("b", 2),("c", 4))

    // Faking input lines with a List of String
    val fakeInput = List("a a", "a b c", "c b c c");

    // Paralleling the fake input collection: resulting on a rdd data structure.
    val inputLinesRDD = sc.parallelize(fakeInput)

    // Paralleling the expected collection: resulting on a rdd data structure.
    val expectedResultRDD = sc.parallelize(expectedResult)

    // Calling the static method wordCount of object WordCount
    val resultRDD = wordCountService.call(inputLinesRDD)


    // Asserts expectedResult rdd  is equal (unordered) to actual result.
    // On failing will make the test to fail.
    assertRDDEquals(expectedResultRDD, resultRDD)
  }

  //Testing WordCount:wordCount using file mock
  test("test static method wordCount of object WordCount using mock data") {
    // Mocking input lines with external file
    val inputMockRdd = sc.textFile("./tmp/mocks/ScalableSentimentClassificationForBigDataPaper.txt")

    // Calling the static method wordCount of object WordCount
    val resultRDD = wordCountService.call(inputMockRdd)

    assert(resultRDD.count === 1694)
  }
}
