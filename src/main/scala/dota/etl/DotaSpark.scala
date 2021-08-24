package dota.etl

import dota.etl.WSClient.DefaultMatchesSize
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object DotaSpark {

  private val spark = SparkSession.builder
    .master("local")
    .appName("DotaSpark")
    //      .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._

  /**
   * Parses matches of a specific player
   *
   * @param text JSON text to read as a DF
   * @param n    number of matches to retrieve, from 1 to 20 as controlled in Main.
   *             Unnecessary default value because of previous logic but we'll keep it just in case.
   * @return First n rows of text.as[DF]
   */
  def parseMatches(text: String, n: Int = DefaultMatchesSize): DataFrame = {
    val dataset: Dataset[String] = spark.createDataset[String](Seq(text))
    spark.read.json(dataset)
      .select("match_id", "player_slot", "radiant_win", "kills", "deaths", "assists")
      .limit(n).toDF()
  }

  /**
   * Parses a particular inspected match
   *
   * @param text JSON text to read as a DF
   * @return
   */
  def parseMatch(text: String): DataFrame = {
    val dataset: Dataset[String] = spark.createDataset[String](Seq(text))

    // These fields are the ones that we will use in order to calculate KPIs
    val interestingFields: Seq[Column] = Seq(
      "player_slot", // To identify the player
      "assists", "deaths", "kills", "isRadiant", // Self explanatory fields
      "win" // 1 if win, 0 otherwise
    ).map( x => col("players." + x)) // Renaming to match the structure name we will give to the players array

    // Parsed DF
    spark.read.json(dataset)
      // Successive selects for readability purposes
      .select(explode($"players").as("players"))
      .select(interestingFields: _*)
  }

  def close(): Unit = spark.close()
}
