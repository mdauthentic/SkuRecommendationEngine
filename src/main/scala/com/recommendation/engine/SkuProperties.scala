package com.recommendation.engine

/**
 * Models the SKU properties
 *
 * @param sku sku name
 * @param attr attributes of each sku
 * */
case class SkuProperties(sku: String, attr: Attributes)


/**
 * SKU attribute model
 * It accepts  list of all attributes (10 in total)
 * */
case class Attributes(a: String, b: String, c: String, d: String, e: String,
                      f: String, g: String, h: String, i: String, j: String) {
  /**
   * Convert all attributes for any given sku into sequence of elements
   */
  def toSeq = Seq(a, b, c, d, e, f, g, h, i, j)

  /**
   * Calculate the similarity between current sku attributes and any other attributes
   *
   * @param attrs the attributes class to compare with
   * @return the weight or similarity score
   */
  def rank(attrs: Attributes): Double = {
    this.toSeq.zip(attrs.toSeq).zipWithIndex
      .filter({ case ((k, v), _) => k.nonEmpty && v.nonEmpty && k == v })
      .map(_._2)
      .foldLeft(0.0)((weight, index) => score(weight, index))
  }

  /**
   * Calculate attribute weight
   *
   * @param weight attribute weight
   * @param index attribute index
   * */
  def score(weight: Double, index: Int): Double = weight + 1.0 + Math.pow(0.5, index.toDouble + 1)

}
