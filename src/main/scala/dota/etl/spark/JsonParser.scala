package dota.etl.spark

import dota.etl.WSClient.DefaultMatchesSize
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, explode, length, when}
import org.apache.spark.sql.types.IntegerType
import play.api.libs.json.Json

object JsonParser extends SharedSparkSession {

  private val direLength = 3

  import spark.implicits._

  /**
   * Parses match history of a specific player
   *
   * @param text JSON text to read as a DF
   * @param n    number of matches to retrieve, from 1 to 20 as controlled in Main.
   *             Unnecessary default value because of previous logic but we'll keep it just in case.
   * @return First n rows of text.as[DF]
   */
  def parsePlayerMatchHistory(text: String, n: Int = DefaultMatchesSize): DataFrame = {
    val dataset: Dataset[String] = spark.createDataset[String](Seq(text))
    spark.read.json(dataset)
      // This columns converts single digit values into Radiant and 1XX values into Dire
      .withColumn("isRadiant", when(length(col("player_slot")) === direLength, false).otherwise(true))
      .select("match_id", "isRadiant", "radiant_win", "kills", "deaths", "assists")
      .limit(n).toDF()
  }

  // These fields are the ones that we will use in order to calculate KPIs
  private val interestingFields: Seq[Column] = $"match_id" +: // To identify the match
    Seq(
      "player_slot", // To identify the player
      "assists", "deaths", "kills", "isRadiant", // Self explanatory fields
      "win" // 1 if win, 0 otherwise
    ).map { x =>
      val name = col("players." + x) // Renaming to match the structure name we will give to the players array
      x match {
        case "isRadiant" => name.as(x) // Flattening the names once selected
        case _ => name.cast(IntegerType).as(x) // Casting unnecessary longs
      }
    }

  /**
   * Parses a particular inspected match
   * In the end it was used as building grounds for parseMatchList
   *
   * @param text JSON text to read as a DF
   * @return DF with interesting fields
   */
  def parseMatch(text: String): DataFrame = {
    val dataset: Dataset[String] = spark.createDataset[String](Seq(text))

    // Parsed DF
    spark.read.json(dataset)
      // Successive selects for readability purposes
      .select($"match_id", explode($"players").as("players"))
      .select(interestingFields: _*)

  }

  /**
   * Parses a list of inspected matches
   *
   * @param text JSON text to read as a DF
   * @return DF with interesting fields
   */
  def parseMatchList(text: String): DataFrame = {
    val dataset: Dataset[String] = spark.createDataset[String](Seq(text))

    // Parsed DF
    spark.read.json(dataset)
      // Successive selects for readability purposes
      .select($"match_id", explode($"players").as("players"))
      .select(interestingFields: _*)

  }

  /**
   * Prettifies a JSON string
   * @param string JSON string
   * @return prettified JSON string
   */
  def prettify(string: String): String = Json.prettyPrint(Json.parse(string))

}
