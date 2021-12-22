package my.learning.apache.spark.beginner.sparksql.dataset

import org.apache.log4j.Level.ERROR
import org.apache.log4j.Logger.getLogger
import org.apache.spark.sql._

object DataframesDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(ars:Array[String]): Unit = {
    getLogger("DataframesDataset").setLevel(ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Dev\\GitRepository\\apache-spark\\scala-spark\\src\\main\\resources\\data\\fakefriends.csv")
      .as[Person]

    println("Here is our inferred schema")
    people.show()

    println("Select name and age")
    people.select("name", "age").show()

    println("Select all teenagers (age <= 21)")
    people.filter(people("age") <= 21).show()

    println("Group by age")
    people.groupBy("age").count().show()

    spark.stop()
  }
}
