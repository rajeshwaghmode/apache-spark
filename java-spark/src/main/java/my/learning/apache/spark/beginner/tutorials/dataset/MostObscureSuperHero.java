package my.learning.apache.spark.beginner.tutorials.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.springframework.util.ResourceUtils.getURL;

public class MostObscureSuperHero {
    public static void main(String...args) throws FileNotFoundException {
        StructType lineSchema = new StructType()
            .add("line", StringType, true);

        StructType superHeroSchema = new StructType()
            .add("id", IntegerType, true)
            .add("name", StringType, true);

        StructType superHeroAppearancesSchema = new StructType()
            .add("id", IntegerType, true)
            .add("appearances", IntegerType, true);

        SparkSession spark = SparkSession
            .builder()
            .appName("MostObscureSuperHero")
            .master("local[*]")
            .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> superHeros = spark.read()
            .option("header", false)
            .option("sep", " ")
            .schema(superHeroSchema)
            .csv(getURL("data/Marvel-names.txt").getPath());
        superHeros.show();

        Dataset<Row> superHeroAppearances = spark.read()
           .option("header", false)
           .schema(lineSchema)
           .csv(getURL("data/Marvel-graph.txt").getPath())
           .withColumn("id", split(col("line"), " ").getItem(0))
           .withColumn("connections", size(split(col("line"), " ")).minus(1))
           .groupBy(col("id"))
           .agg(sum("connections").cast(IntegerType).alias("appearances"));

        superHeroAppearances.sort(col("appearances").asc()).show();

        int minAppearanceCount = superHeroAppearances
            .agg(min("appearances"))
            .first()
            .getInt(0);

        Dataset<Row> superHerosWithMinAppearances = superHeroAppearances.filter(col("appearances").equalTo(minAppearanceCount));

        Dataset<Row> mostObscureSuperHeroNames = superHerosWithMinAppearances
           .join(superHeros, superHerosWithMinAppearances.col("id").equalTo(superHeros.col("id")))
           .select(col("name"));

        System.out.printf("\nThe following characters have only %d appearance(s)\n", minAppearanceCount);
        mostObscureSuperHeroNames.show();
    }
}
