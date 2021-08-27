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

  val player: DataFrame = {
    val path = s"src/test/resources/player.csv"

    val matchSchema: StructType = StructType(Seq(
      StructField("match_id", LongType, nullable = true),
      StructField("player_slot", IntegerType, nullable = true),
      StructField("radiant_win", BooleanType, nullable = true),
      StructField("kills", IntegerType, nullable = true),
      StructField("deaths", IntegerType, nullable = true),
      StructField("assists", IntegerType, nullable = true),
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

  test("playerPerformance method") {

    val data = Seq(
      // scalastyle:off
      Row(6135452599L, 2, 9, 1.5),
      Row(6135408545L, 1, 6, 0.5),
      Row(6134018090L, 132, 14, 1.4),
      Row(6127211062L, 129, 10, 3.3333333333333335),
      Row(6126666493L, 0, 35, 11.666666666666666),
      Row(6126623611L, 130, 7, 1.1666666666666667),
      Row(6126574298L, 2, 18, 2.5714285714285716),
      Row(6125948851L, 130, 19, 1.7272727272727273),
      Row(6125877819L, 128, 6, 0.5454545454545454),
      Row(6125833122L, 0, 18, 18.0)
      // scalastyle:on
    )

    val actual = DataAnalytics.playerPerformance(player)
    val schema = actual.schema
    val expected = DataAnalytics.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }

  test("completePerformance method") {

    val data = Seq(
      // scalastyle:off
      Row(6135452599L, 9, 1.5, 50.0, true, 0),
      Row(6135452599L, 9, 1.5, 24.324324324324326, false, 1)
      // scalastyle:on
    )

    val actual = DataAnalytics.completePerformance(
      DataAnalytics.teamKills(inspectedMatch), // These two dfs should be built up from scratch to preserve the integrity of unit testing
      DataAnalytics.playerPerformance(player) // But I have already done it twice in this class so you get the idea ;)
    )
    val schema = actual.schema
    val expected = DataAnalytics.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }
}
