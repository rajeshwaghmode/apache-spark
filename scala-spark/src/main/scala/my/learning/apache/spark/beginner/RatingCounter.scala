package my.learning.apache.spark.beginner

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
    val lines = sc.textFile(file)
    val ratings = lines.map(s => s.split(",")(1))

    ratings.countByValue().foreach(println)
  }
}
