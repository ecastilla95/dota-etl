package dota.etl.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait SharedSparkSession {

  protected val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("SharedSparkSession")
    .getOrCreate()

  Logger.getLogger("org.apache").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /**
   * Reads a DataFrame with the given schema from the given path
   * @param path path to read the DataFrame from
   * @param schema schema of the DataFrame
   * @return DataFrame
   */
  def readDf(path: String, schema: StructType): DataFrame = spark.read
    .option("header", value = true)
    .schema(schema)
    .csv(path)

  /**
   * Creates a dataframe from a Sequence of Rows with the given schema
   * @param data Sequence of Rows
   * @param schema schema of the DataFrame
   * @return DataFrame
   */
  def createDataFrame(data: Seq[Row], schema: StructType): DataFrame = {
    val rdd = spark.sparkContext.parallelize(data)
    spark.createDataFrame(rdd, schema)
  }

  /**
   * Close the current SparkSession
   */
  def close(): Unit = spark.close()
}
