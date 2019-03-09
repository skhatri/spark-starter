package com.github.skhatri

object SparkExample extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL to View EPL results")
    .config("spark.driver.host", "localhost")
    .master("local[2]")
    .getOrCreate()


  import spark.implicits._

  spark.read.option("header", true)
    .csv("epl.csv")
    .show()
}
