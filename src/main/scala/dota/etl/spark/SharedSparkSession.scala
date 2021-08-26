package dota.etl.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait SharedSparkSession {

  protected val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("SharedSparkSession")
    .getOrCreate()

  def readDf(path: String, schema: StructType): DataFrame = spark.read
    .option("header", value = true)
    .schema(schema)
    .csv(path)

  def createDataFrame(data: Seq[Row], schema: StructType): DataFrame = {
    val rdd = spark.sparkContext.parallelize(data)
    spark.createDataFrame(rdd, schema)
  }

  def close(): Unit = spark.close()
}
