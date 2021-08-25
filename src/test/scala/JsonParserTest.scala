import com.github.mrpowers.spark.fast.tests.DatasetComparer
import dota.etl.JsonParser
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class JsonParserTest extends AnyFunSuite with DatasetComparer {

  /**
   * JSON reading utils
   */

  def getJsonString(fileName: String): String = {
    val source = Source.fromFile(s"src/test/resources/$fileName.json")
    source.getLines().reduce(_ + _)
  }

  val playerMatches: String = getJsonString("player")
  val inspectedMatch: String = getJsonString("match")

  /**
   * JSON reading tests
   */

  test("parseMatches default number of records test") {

    val count = JsonParser.parseMatches(playerMatches).count().toInt
    println(s"For default length we select the first ten records")
    assert(count == 10)

  }

  test("parseMatches top n records test") {

    assert {
      (1 to 20).forall {
        l =>
          val count = JsonParser.parseMatches(playerMatches, l).count().toInt
          println(s"For length $l we select the first $count records")
          count == l
      }
    }

  }

  test("parseMatch") {

    val data = Seq(
      // scalastyle:off
      Row(6135452599L, 0, 4, 6, 5, true, 0),
      Row(6135452599L, 1, 9, 11, 0, true, 0),
      Row(6135452599L, 2, 8, 6, 1, true, 0),
      Row(6135452599L, 3, 5, 8, 3, true, 0),
      Row(6135452599L, 4, 4, 6, 9, true, 0),
      Row(6135452599L, 128, 17, 5, 4, false, 1),
      Row(6135452599L, 129, 9, 3, 5, false, 1),
      Row(6135452599L, 130, 7, 3, 8, false, 1),
      Row(6135452599L, 131, 10, 3, 14, false, 1),
      Row(6135452599L, 132, 19, 4, 6, false, 1)
      // scalastyle:on
    )

    val actual = JsonParser.parseMatch(inspectedMatch)
    val schema = actual.schema
    val expected = JsonParser.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }

}
