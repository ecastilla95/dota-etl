package dota.etl

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DotaSpark(value: String) {

    val spark = SparkSession.builder
      .master("local")
      .appName("DotaETL")
//      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val dataset: Dataset[String] = spark.createDataset[String](Seq(value))
    val df = spark.read.json(dataset).select("match_id", "player_slot", "radiant_win", "kills", "deaths", "assists")
    df.show()

}