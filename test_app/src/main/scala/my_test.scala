import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties



object my_test extends App{


  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def readCSV(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("header", "true")
      .option("delimiter",",")
      .option("inferSchema", "true")
      .csv(path)

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .config("spark.sql.autoBroadcastJoinThreshold", 0)
    .config("spark.sql.adaptive.enabled", value = false)
    .getOrCreate()

  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame) =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total trips"),
        round(min("trip_distance"), 2).as("min distance"),
        round(mean("trip_distance"), 2).as("mean distance"),
        round(max("trip_distance"), 2).as("max distance")
      )
      .orderBy(col("total trips").desc)


//  val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/taxi_zones.csv")
  taxiZoneDF.show()
  taxiZoneDF.write.mode("overwrite").options(Map("header"->"true", "delimiter"->",")).csv("E:/Spark Developer OTUS/test/out_test")

  taxiZoneDF.printSchema()
  val taxiFactsDFReade: DataFrame = readCSV("E:/Spark Developer OTUS/test/out_test")
  taxiFactsDFReade.printSchema()
  taxiFactsDFReade.show()
  spark.stop()
//  val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

//  result.explain(true)
//
//  result.show()

//  Thread.sleep(100000)
}
