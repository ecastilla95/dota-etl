package dota.etl.spark

import org.apache.spark.sql._

object DataAnalytics extends SharedSparkSession {

  def teamKills(df: DataFrame): DataFrame = df
    .withColumnRenamed("kills", "team_kills")
    .groupBy("match_id", "isRadiant", "win")
    .sum("team_kills")

  def teamKills(df: DataFrame): DataFrame = df
    .withColumnRenamed("kills", "team_kills")
    .groupBy("match_id", "isRadiant", "win")
    .sum("team_kills")

}
