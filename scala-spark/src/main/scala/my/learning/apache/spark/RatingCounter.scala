package my.learning.apache.spark

import org.apache.spark.sql.SparkSession


object RatingCounter {
  def main(args:Array[String]): Unit = {
//    val sc = new SparkContext("local[*]", "RatingCounter")
    val sc = SparkSession
      .builder
      .appName("Apache Spark RatingCounter")
      .master("local[*]")
      .getOrCreate
      .sparkContext

    val lines = sc.textFile("C:/Dev/GitRepository/apache-spark/spark-scala/src/main/resources/data/movie/MovieRating.txt")
    val ratings = lines.map(s => s.split(",")(1))
    ratings.countByValue().foreach(println)

  }
}
