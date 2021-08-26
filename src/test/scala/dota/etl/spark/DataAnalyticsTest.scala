package dota.etl.spark

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DataAnalyticsTest extends AnyFunSuite with DatasetComparer {

  /**
   * Dataframe reading utils
   */
  val inspectedMatch: DataFrame = {
    val path = s"src/test/resources/match.csv"

    val matchSchema: StructType = StructType(Seq(
      StructField("match_id", LongType, nullable = true),
      StructField("player_slot", IntegerType, nullable = true),
      StructField("assists", IntegerType, nullable = true),
      StructField("deaths", IntegerType, nullable = true),
      StructField("kills", IntegerType, nullable = true),
      StructField("isRadiant", BooleanType, nullable = true),
      StructField("win", IntegerType, nullable = true)
    ))

    DataAnalytics.readDf(path, matchSchema)
  }

  /**
   * Dataframe analytics tests
   */
  test("teamKills method") {

    val data = Seq(
      // scalastyle:off
      Row(6135452599L, false, 1, 37L),
      Row(6135452599L, true, 0, 18L)
      // scalastyle:on
    )

    val actual = DataAnalytics.teamKills(inspectedMatch)
    val schema = actual.schema
    val expected = DataAnalytics.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }
}
