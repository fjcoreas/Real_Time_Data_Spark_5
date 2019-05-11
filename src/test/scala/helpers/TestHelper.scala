package helpers

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

trait TestHelper extends BeforeAndAfterEach with SharedSparkContext {
  self: Suite =>

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
}
