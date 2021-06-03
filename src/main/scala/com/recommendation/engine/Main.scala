package com.recommendation.engine

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import scala.io.StdIn

object Main extends App {

  // start Spark session
  val spark = SparkSession
    .builder()
    .appName("SKU Recommendation Engine")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // Set spark events reporting at an acceptable level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  if (args.isEmpty) {
    println("File path argument missing.")
    sys.exit()
  }

  final val path = args(0)

  while (true) {
    try {
      println("\t-------------- Press ':q' to exit --------------")

      val skuId = StdIn.readLine("Enter SKU > ")

      if (skuId == ":q" || skuId == "") {
        spark.stop()
        sys.exit()
      }

      val skuDF = SparkJob.readJson(spark, path) // read dataset

      val skuDSWithSchema = SparkJob.toDsSchema(spark, skuDF)

      val skuProp = SparkJob.getSku(skuId, skuDSWithSchema)
      // calculate similarity
      SparkJob.computeSimilarity(skuProp, skuDSWithSchema)
        .show(10, truncate = false)
    } catch {
      case e: RuntimeException =>
        println(e.getMessage)
        spark.stop()
    }
  }

  spark.stop()

}
