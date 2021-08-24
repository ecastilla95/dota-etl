import dota.etl.DotaSpark
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class SparkTest extends AnyFunSuite {

  /**
   * JSON reading utils
   */

  def getJsonString(fileName: String) : String = {
    val source = Source.fromFile(s"src/test/resources/$fileName.json")
    source.getLines().reduce(_ + _)
  }

  val playerMatches: String = getJsonString("player")
  val inspectedMatch: String = getJsonString("match")

  /**
   * JSON reading tests
   */

  test("parseMatches default number of records test") {

    val count = DotaSpark.parseMatches(playerMatches).count().toInt
    println(s"For default length we select the first ten records")
    assert (count == 10)

  }

  test("parseMatches top n records test") {

    assert {
      (1 to 20).forall {
        l =>
          val count = DotaSpark.parseMatches(playerMatches, l).count().toInt
          println(s"For length $l we select the first $count records")
          count == l
      }
    }

  }

  test("parseMatch") {
    val df = DotaSpark.parseMatch(inspectedMatch)
    df.show()
  }

}
