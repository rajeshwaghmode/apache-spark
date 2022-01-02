package my.learning.apache.spark.beginner.tutorials.dataset;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;

import static org.apache.spark.sql.functions.*;
import static org.springframework.util.ResourceUtils.getURL;

public class MostPopularSuperHero {
    public static void main(String... args) throws FileNotFoundException {
        StructType lineSchema = new StructType()
            .add("line", DataTypes.StringType, true);

        StructType superHeroSchema = new StructType()
            .add("id",  DataTypes.IntegerType, true)
            .add("name", DataTypes.StringType, true);

        StructType superHeroConnectionSchema = new StructType()
            .add("id",  DataTypes.StringType, true)
            .add("connections", DataTypes.StringType, true);

        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("MostPopularSuperHero")
            .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> superHeros = spark.read()
            .schema(superHeroSchema)
            .option("sep", " ")
            .csv(getURL("data/Marvel-names.txt").getPath());
        superHeros.show();

        Dataset<Row> superHeroConnections = spark.read()
            .schema(lineSchema)
            .csv(getURL("data/Marvel-graph.txt").getPath())
            .withColumn("id", split(col("line"), " ").getItem(0))
            .withColumn("connections", size(split(col("line"), " ")).minus(1))
            .groupBy("id")
            .agg(sum("connections").alias("connections"));
        superHeroConnections.show();

        Row mostPopularSuperHeroAppearances = superHeroConnections
                .sort(col("connections").desc())
                .first();

        Row mostPopularSuperHeroName = superHeros
           .filter(col("id").equalTo(mostPopularSuperHeroAppearances.getAs("id")))
           .select("name")
           .first();

        System.out.printf("\n%s is the most popular super hero with %d co-appearances\n", mostPopularSuperHeroName.getAs("name"), mostPopularSuperHeroAppearances.getAs("connections"));
    }
}
