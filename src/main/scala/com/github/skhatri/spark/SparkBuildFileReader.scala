package com.github.skhatri.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkExample extends App {

  val sc = new SparkContext(new SparkConf().setAppName("myapp").setMaster("local[2]"))
  val lines = sc.textFile("build.gradle")

  val linesWithCompile: RDD[String] = lines.filter(line => line.contains("compile ")).cache()

  //first usage of rdd
  println(linesWithCompile.count())

  //second usage of rdd
  val CompileDep =
    """.*compile "(.*)"""".r

  val groupIds = linesWithCompile.map(compileLine => compileLine match {
    case CompileDep(dep) => dep
  }).map(depLine => depLine.split(":"))
    .map(a => a(0)).cache()

  groupIds.groupBy(s => s)
    .mapValues(s => s.size).sortBy(stringWithCount => stringWithCount._2, false)
    .foreach(stringWithCounts => println("string: " + stringWithCounts._1 + ", count: " + stringWithCounts._2))


}
