package dota.etl.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataAnalytics extends SharedSparkSession {

  import spark.implicits._

  private val percent = 100

  def extractIds(df: DataFrame): Array[Long] =
    df.select("match_id").distinct()
      .map(row => row.getAs[Long]("match_id"))
      .collect()

  def teamKills(df: DataFrame): DataFrame = df
    .groupBy("match_id", "isRadiant", "win")
    .agg(sum($"kills").as("team_kills"))

  def playerPerformance(df: DataFrame): DataFrame = df
    .withColumn("takedowns", $"kills" + $"assists")
    .withColumn("kda", $"takedowns" / $"deaths")
    .select("match_id", "isRadiant", "takedowns", "kda")

  def completePerformance(tk: DataFrame, pp: DataFrame): DataFrame = pp
    .join(tk,
      (pp("match_id") === tk("match_id")) and
        (pp("isRadiant") === tk("isRadiant"))
    ).withColumn("kp", ($"takedowns" / $"team_kills") * percent)
    .drop(tk("match_id")).drop(tk("isRadiant"))
    .select("match_id", "takedowns", "kda", "kp", "isRadiant", "win")

  def summary(df: DataFrame): DataFrame = df
    .withColumn("radiant_count", when($"isRadiant", "1"))
    .withColumn("dire_count", when(not($"isRadiant"), "1"))
    .withColumn("radiant_kda", when($"isRadiant", $"kda"))
    .withColumn("dire_kda", when(not($"isRadiant"), $"kda"))
    .agg(
      max("kda").as("max_kda"),
      min("kda").as("min_kda"),
      avg("kda").as("avg_kda"),
      max("kp").as("max_kp"),
      min("kp").as("min_kp"),
      avg("kp").as("avg_kp"),
      count("radiant_count").as("games_as_radiant"),
      max("radiant_kda").as("radiant_max_kda"),
      min("radiant_kda").as("radiant_min_kda"),
      avg("radiant_kda").as("radiant_avg_kda"),
      count("dire_count").as("games_as_dire"),
      max("dire_kda").as("dire_max_kda"),
      min("dire_kda").as("dire_min_kda"),
      avg("dire_kda").as("dire_avg_kda"),
    )

  def toJson(df: DataFrame): String = df
    .select(to_json(struct(col("*"))).alias("summary"))
    .first() // We know there is only one record
    .mkString("")

  // Functional showoff
  val analyze: ((DataFrame, DataFrame)) => String = {
    data => // data is composed of two dataframes: playerMatchHistory and parsedMatches
      // We create this function that applies the playerPerformance method to df playerMatchHistory
      // and the teamKills method to df parsedMatches
      val separatePerformances: (DataFrame, DataFrame) => (DataFrame, DataFrame) =
        (playerMatchHistory, parsedMatches) => (playerPerformance(playerMatchHistory), teamKills(parsedMatches))
      // Then we compose all these functions:
      // 1) separatePerformances with input data and output a duple (playerPerformance, teamKills)
      // 2) completePerformance joins these two dfs into the complete performance df
      // 3) from this df we calculate the summary information and we convert to JSON
      (separatePerformances.tupled andThen (completePerformance _).tupled andThen summary andThen toJson)(data)
  }
}
