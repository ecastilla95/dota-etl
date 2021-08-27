package dota.etl.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, sum}

object DataAnalytics extends SharedSparkSession {

  import spark.implicits._

  private val percent = 100

  def extractIds(df: DataFrame): Array[Long] =
    df.select("match_id", "player_slot").distinct()
      .map(row => row.getAs[Long]("match_id"))
      .collect()

  def teamKills(df: DataFrame): DataFrame = df
    .groupBy("match_id", "isRadiant", "win")
    .agg(sum($"kills").as("team_kills"))

  def playerPerformance(df: DataFrame): DataFrame = df
    .withColumn("takedowns", $"kills" + $"assists")
    .withColumn("kda", $"takedowns" / $"deaths")
    .select("match_id", "player_slot", "takedowns", "kda")

  def completePerformance(tk: DataFrame, pp: DataFrame): DataFrame = pp
    .join(tk, pp("match_id") === tk("match_id"))
    .withColumn("kp", ($"takedowns" / $"team_kills") * percent)
    .drop(tk("match_id"))
    .select("match_id", "takedowns", "kda", "kp", "isRadiant", "win")

}
