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
      StructField("isRadiant", BooleanType, nullable = true),
      StructField("radiant_win", BooleanType, nullable = true),
      StructField("kills", IntegerType, nullable = true),
      StructField("deaths", IntegerType, nullable = true),
      StructField("assists", IntegerType, nullable = true)
    ))

    DataAnalytics.readDf(path, matchSchema)
  }

  val completePerformance: DataFrame = {
    val path = s"src/test/resources/complete_performance.csv"

    val matchSchema: StructType = StructType(Seq(
      StructField("match_id", LongType, nullable = true),
      StructField("takedowns", IntegerType, nullable = true),
      StructField("kda", DoubleType, nullable = true),
      StructField("kp", DoubleType, nullable = true),
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

  test("playerPerformance method") {

    val data = Seq(
      // scalastyle:off
      Row(6135452599L, true, 9, 1.5),
      Row(6135408545L, true, 6, 0.5),
      Row(6134018090L, false, 14, 1.4),
      Row(6127211062L, false, 10, 3.3333333333333335),
      Row(6126666493L, true, 35, 11.666666666666666),
      Row(6126623611L, false, 7, 1.1666666666666667),
      Row(6126574298L, true, 18, 2.5714285714285716),
      Row(6125948851L, false, 19, 1.7272727272727273),
      Row(6125877819L, false, 6, 0.5454545454545454),
      Row(6125833122L, true, 18, 18.0)
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
      Row(6135452599L, 9, 1.5, 50.0, true, 0)
      // scalastyle:on
    )

    val actual = DataAnalytics.completePerformance(
      DataAnalytics.teamKills(inspectedMatch), // These two dfs should be built up from scratch to preserve the integrity of unit testing
      DataAnalytics.playerPerformance(player) // But I have already done it multiple times in this class so you get the idea ;)
    )
    val schema = actual.schema
    val expected = DataAnalytics.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }

  test("summary method") {

    val data = Seq(
      // scalastyle:off
      Row(1.5, 0.5, 1.0, 50.0, 22.22222222222222, 36.111111111111114, 2L, 1.5, 0.5, 1.0, 0L, null, null, null)
      // scalastyle:on
    )

    val actual = DataAnalytics.summary(completePerformance)
    val schema = actual.schema
    val expected = DataAnalytics.createDataFrame(data, schema)

    assertSmallDatasetEquality(actual, expected)
  }

  test("toJson method") {

    // scalastyle:off
    val expected = """{"max_kda":1.5,"min_kda":0.5,"avg_kda":1.0,"max_kp":50.0,"min_kp":22.22222222222222,"avg_kp":36.111111111111114,"games_as_radiant":2,"radiant_max_kda":1.5,"radiant_min_kda":0.5,"radiant_avg_kda":1.0,"games_as_dire":0}"""
    // scalastyle:on

    val actual = DataAnalytics.toJson(
    DataAnalytics.summary(completePerformance)
    )

    assert(actual == expected)
  }
}
