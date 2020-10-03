package my.learning.apache.spark.beginner.tutorials;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.springframework.util.ResourceUtils;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

//This tutorial only focus on Spark 2+.
public class TextFileWordCount2 {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String... args) throws FileNotFoundException {
        SparkSession spark = SparkSession
                .builder()
                .appName("TextFileWordCount2")
                .master("local")
                .getOrCreate();

        //Reading from single file
        Dataset<String> lines = spark.read().text(ResourceUtils.getURL("classpath:TextFile1.txt").getPath()).as(Encoders.STRING());
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                                    public Iterator<String> call(String s) {
                                        return Arrays.asList(s.split(" ")).iterator();
                                    }
                                }, Encoders.STRING());

        Dataset<Row> output1 = words.toDF("Word")
             .groupBy("Word")
             .count()
             .withColumnRenamed("count", "WordCount")
             .sort("WordCount");
        System.out.println("Word Count for file TextFile1.txt");
        output1.show();

        //Read from multitple files
        Dataset<String> dataset1 = spark.read().textFile(ResourceUtils.getURL("classpath:TextFile1.txt").getPath()).as(Encoders.STRING());
        Dataset<String> dataset2 = spark.read().textFile(ResourceUtils.getURL("classpath:TextFile2.txt").getPath()).as(Encoders.STRING());
        Dataset<String> dataset3 = spark.read().textFile(ResourceUtils.getURL("classpath:TextFile3.txt").getPath()).as(Encoders.STRING());

        Dataset<String> combinedLines = dataset1.unionAll(dataset2).unionAll(dataset3);
        Dataset<String> combinedWords = combinedLines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> output2 = combinedWords.toDF("Word")
                .groupBy("Word")
                .count()
                .withColumnRenamed("count", "WordCount")
                .sort("WordCount");
        System.out.println("Word Count for multiple files - TextFile1.txt, TextFile2.txt, TextFile3.txt");
        output2.show();
    }
}
