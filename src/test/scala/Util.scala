import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class Util extends FunSuite with SharedSparkContext{
  test( testName = "string") {
    val lines = sc.textFile("./data/unsd-citypopulation-year-both.txt")
    lines.filter(line => line.contains("Male" )).saveAsTextFile("./data/city_male_population")
    lines.filter(line => line.contains("Female")).saveAsTextFile("./data/city_female_population")
  }

}
