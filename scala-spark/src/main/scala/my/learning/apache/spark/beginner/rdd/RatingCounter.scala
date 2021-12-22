package my.learning.apache.spark.beginner.rdd

import org.apache.spark.sql.SparkSession

object RatingCounter {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder
      .appName("Apache Spark RatingCounter")
      .master("local[*]")
      .getOrCreate
      .sparkContext

    val file = getClass.getResource("/data/movie/MovieRating.txt").getFile()
    sc.textFile(file)
      .map(s => s.split(",")(1))
      .filter(s => s != null || !s.isBlank)
      .countByValue()
      .foreach(println)
  }
}
