package com.recommendation.engine

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkJob {

  /**
   * Read data from file
   *
   * @param spark: spark session
   * @param path:  file path
   *
   * @return dataframe of sku data
   * */
  def readJson(spark: SparkSession, path: String): DataFrame = spark.read.json(path).persist(StorageLevel.MEMORY_AND_DISK)

  /**
   * Rename `SKU` dataframe columns for easier reading,
   * convert resulting `DataFrame` to `Dataset`,then map to schema
   *
   * @param spark: spark session
   * @param skuDF: sku dataframe
   *
   * @return sku dataset
   * */
  def toDsSchema(spark: SparkSession, skuDF: DataFrame): Dataset[SkuProperties] = {
    import spark.implicits._
    skuDF.select($"sku", struct($"attributes.att-a".as("a"), $"attributes.att-b".as("b"),
      $"attributes.att-c".as("c"), $"attributes.att-d".as("d"), $"attributes.att-e".as("e"),
      $"attributes.att-f".as("f"), $"attributes.att-g".as("g"), $"attributes.att-h".as("h"),
      $"attributes.att-i".as("i"), $"attributes.att-j".as("j")).alias("attr"))
      .drop("attributes").as[SkuProperties]
  }

  /**
   * Get properties of a given sku
   *
   * @param skuID: key of sku to retrieve
   * @param skuDF: sku dataframe
   *
   * @return sku properties for given sku key
   * */
  def getSku(skuID: String, skuDF: Dataset[SkuProperties]): SkuProperties = {
    val getSkuDetails = skuDF.filter(s"sku == '$skuID'").take(1)
    if (getSkuDetails.isEmpty) throw new Exception(s"$skuID not found.")
    getSkuDetails.head
  }

  /**
   * Compute the sku similarity based on their attribute
   *
   * @param spark: spark session
   * @param skuID: sku key
   * @param skuDS: sku dataset
   *
   * @return dataset of similar skus
   * */
  def computeSimilarity(spark: SparkSession, skuID: String, skuDS: Dataset[SkuProperties]): Dataset[Row] = {
    import spark.implicits._
    // get sku attribute
    val getSkuAttr = getSku(skuID, skuDS).attr
    // we do not want to compare the interested sku with itself, so filter it out
    skuDS.filter(s"sku !='$skuID'").map(e => {
      val similarity = (e.sku, e.attr.rank(getSkuAttr))
      similarity
    }).withColumnRenamed("_1", "Similar SKU")
      .withColumnRenamed("_2", "Ranking")
      .orderBy(col("Ranking").desc) // order by the most similar sku
  }

  /**
   * Compute the sku similarity based on their attribute.
   * Assign numbers 9 to 0 in a left to right manner to each attribute,
   * then rank using Spark's inbuilt `rank.over()` function
   *
   * @param r: sku of interest
   * @param skuDS: sku dataset
   *
   * @return dataset of similar skus
   * */
  def computeSimilarity(r: SkuProperties, skuDS: Dataset[SkuProperties]): Dataset[Row] = {

    val (a, b, c, d, e) = (r.attr.a, r.attr.b, r.attr.c, r.attr.d, r.attr.e)
    val (f, g, h, i, j) = (r.attr.f, r.attr.g, r.attr.h, r.attr.i, r.attr.j)

    skuDS.filter(s"sku !='$r.sku'")
      .withColumn("col_a",  when(col("attr.a") === s"$a",9).otherwise(-1))
      .withColumn("col_b",  when(col("attr.b") === s"$b",8).otherwise(-1))
      .withColumn("col_c",  when(col("attr.c") === s"$c",7).otherwise(-1))
      .withColumn("col_d",  when(col("attr.d") === s"$d",6).otherwise(-1))
      .withColumn("col_e",  when(col("attr.e") === s"$e",5).otherwise(-1))
      .withColumn("col_f",  when(col("attr.f") === s"$f",4).otherwise(-1))
      .withColumn("col_g",  when(col("attr.g") === s"$g",3).otherwise(-1))
      .withColumn("col_h",  when(col("attr.h") === s"$h",2).otherwise(-1))
      .withColumn("col_i",  when(col("attr.i") === s"$i",1).otherwise(-1))
      .withColumn("col_j",  when(col("attr.j") === s"$j",0).otherwise(-1))
      .withColumn("all_col",
        regexp_replace(concat(col("col_a"), col("col_b"), col("col_c"),
          col("col_d"), col("col_e"), col("col_f"),
          col("col_g"), col("col_h"), col("col_i"),
          col("col_j")).cast(StringType), "-1", "").cast(IntegerType))
      .withColumn("rank", rank.over(Window.orderBy("all_col"))) // .partitionBy("col_a")
      .orderBy(col("rank").desc)
      .select(col("sku").alias("Most Similar SKU's"), col("rank").alias("Rank"))
  }

}
