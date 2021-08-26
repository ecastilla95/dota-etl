package dota.etl.spark

import com.github.mrpowers.spark.fast.tests.DatasetComparer
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
  val inspectedMatchList: String = getJsonString("match_list")

  /**
   * JSON reading tests
   */

  test("parsePlayerMatchHistory default number of records test") {

    val count = JsonParser.parsePlayerMatchHistory(playerMatches).count().toInt
    println(s"For default length we select the first ten records")
    assert(count == 10)

  }

  test("parsePlayerMatchHistory top n records test") {

    assert {
      (1 to 20).forall {
        l =>
          val count = JsonParser.parsePlayerMatchHistory(playerMatches, l).count().toInt
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

  test("parseMatchList") {

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
      Row(6135452599L, 132, 19, 4, 6, false, 1),
      Row(6135408545L, 0, 7, 9, 10, true, 0),
      Row(6135408545L, 1, 5, 12, 1, true, 0),
      Row(6135408545L, 2, 8, 9, 6, true, 0),
      Row(6135408545L, 3, 4, 8, 6, true, 0),
      Row(6135408545L, 4, 7, 8, 4, true, 0),
      Row(6135408545L, 128, 11, 4, 20, false, 1),
      Row(6135408545L, 129, 16, 6, 5, false, 1),
      Row(6135408545L, 130, 9, 4, 7, false, 1),
      Row(6135408545L, 131, 13, 7, 7, false, 1),
      Row(6135408545L, 132, 20, 7, 5, false, 1),
      Row(6134018090L, 0, 15, 5, 9, true, 1),
      Row(6134018090L, 1, 20, 6, 5, true, 1),
      Row(6134018090L, 2, 20, 5, 11, true, 1),
      Row(6134018090L, 3, 15, 10, 15, true, 1),
      Row(6134018090L, 4, 14, 7, 12, true, 1),
      Row(6134018090L, 128, 9, 10, 11, false, 0),
      Row(6134018090L, 129, 9, 12, 4, false, 0),
      Row(6134018090L, 130, 10, 13, 4, false, 0),
      Row(6134018090L, 131, 10, 7, 6, false, 0),
      Row(6134018090L, 132, 9, 10, 5, false, 0),
      Row(6127211062L, 0, 22, 6, 2, true, 1),
      Row(6127211062L, 1, 25, 10, 3, true, 1),
      Row(6127211062L, 2, 7, 2, 21, true, 1),
      Row(6127211062L, 3, 12, 8, 5, true, 1),
      Row(6127211062L, 4, 12, 6, 7, true, 1),
      Row(6127211062L, 128, 12, 11, 3, false, 0),
      Row(6127211062L, 129, 8, 3, 2, false, 0),
      Row(6127211062L, 130, 11, 9, 4, false, 0),
      Row(6127211062L, 131, 9, 8, 11, false, 0),
      Row(6127211062L, 132, 6, 9, 9, false, 0),
      Row(6126666493L, 0, 31, 3, 4, true, 1),
      Row(6126666493L, 1, 13, 6, 23, true, 1),
      Row(6126666493L, 2, 18, 4, 10, true, 1),
      Row(6126666493L, 3, 27, 6, 6, true, 1),
      Row(6126666493L, 4, 14, 3, 9, true, 1),
      Row(6126666493L, 128, 6, 16, 2, false, 0),
      Row(6126666493L, 129, 10, 7, 7, false, 0),
      Row(6126666493L, 130, 7, 9, 8, false, 0),
      Row(6126666493L, 131, 11, 13, 1, false, 0),
      Row(6126666493L, 132, 10, 7, 3, false, 0),
      Row(6126623611L, 0, 23, 1, 10, true, 1),
      Row(6126623611L, 1, 14, 2, 11, true, 1),
      Row(6126623611L, 2, 17, 1, 8, true, 1),
      Row(6126623611L, 3, 22, 5, 6, true, 1),
      Row(6126623611L, 4, 16, 2, 11, true, 1),
      Row(6126623611L, 128, 1, 7, 3, false, 0),
      Row(6126623611L, 129, 7, 11, 0, false, 0),
      Row(6126623611L, 130, 6, 6, 1, false, 0),
      Row(6126623611L, 131, 6, 9, 4, false, 0),
      Row(6126623611L, 132, 3, 13, 1, false, 0),
      Row(6126574298L, 0, 14, 2, 17, true, 1),
      Row(6126574298L, 1, 23, 9, 6, true, 1),
      Row(6126574298L, 2, 13, 7, 5, true, 1),
      Row(6126574298L, 3, 15, 9, 12, true, 1),
      Row(6126574298L, 4, 17, 5, 8, true, 1),
      Row(6126574298L, 128, 13, 7, 6, false, 0),
      Row(6126574298L, 129, 15, 10, 2, false, 0),
      Row(6126574298L, 130, 10, 11, 19, false, 0),
      Row(6126574298L, 131, 17, 11, 3, false, 0),
      Row(6126574298L, 132, 16, 10, 2, false, 0),
      Row(6125948851L, 0, 29, 9, 3, true, 1),
      Row(6125948851L, 1, 16, 7, 10, true, 1),
      Row(6125948851L, 2, 10, 2, 27, true, 1),
      Row(6125948851L, 3, 30, 9, 4, true, 1),
      Row(6125948851L, 4, 27, 7, 9, true, 1),
      Row(6125948851L, 128, 14, 15, 2, false, 0),
      Row(6125948851L, 129, 9, 10, 14, false, 0),
      Row(6125948851L, 130, 13, 11, 6, false, 0),
      Row(6125948851L, 131, 8, 13, 3, false, 0),
      Row(6125948851L, 132, 6, 7, 7, false, 0),
      Row(6125877819L, 0, 20, 2, 6, true, 1),
      Row(6125877819L, 1, 16, 2, 17, true, 1),
      Row(6125877819L, 2, 22, 11, 5, true, 1),
      Row(6125877819L, 3, 15, 4, 32, true, 1),
      Row(6125877819L, 4, 35, 3, 1, true, 1),
      Row(6125877819L, 128, 3, 11, 3, false, 0),
      Row(6125877819L, 129, 9, 12, 2, false, 0),
      Row(6125877819L, 130, 9, 17, 6, false, 0),
      Row(6125877819L, 131, 5, 7, 7, false, 0),
      Row(6125877819L, 132, 5, 15, 4, false, 0),
      Row(6125833122L, 0, 13, 1, 5, true, 1),
      Row(6125833122L, 1, 13, 1, 4, true, 1),
      Row(6125833122L, 2, 8, 4, 11, true, 1),
      Row(6125833122L, 3, 13, 2, 4, true, 1),
      Row(6125833122L, 4, 13, 3, 4, true, 1),
      Row(6125833122L, 128, 4, 9, 0, false, 0),
      Row(6125833122L, 129, 2, 6, 0, false, 0),
      Row(6125833122L, 130, 0, 4, 2, false, 0),
      Row(6125833122L, 131, 5, 5, 0, false, 0),
      Row(6125833122L, 132, 1, 6, 7, false, 0),
      // scalastyle:on
    )

    val actual = JsonParser.parseMatchList(inspectedMatchList)
    val schema = actual.schema
    val expected = JsonParser.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }

}
