package my.learning.apache.spark.beginner.tutorials;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.util.ResourceUtils;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

//This tutorial only focus on Spark 2 and lower versions of it.
public class TextFileWordCount1 {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String... args) throws FileNotFoundException {
        SparkConf sparkConf = new SparkConf().setAppName("TextFileWordCount1").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //Reading from single file
        JavaRDD<String> lines = sparkContext.textFile(ResourceUtils.getURL("classpath:TextFile1.txt").getPath());
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCount = ones.reduceByKey((count1, count2) -> count1 + count2);
        List<Tuple2<String, Integer>> result1 = wordCount.collect();

        System.out.println("Word Count for file TextFile1.txt");
        result1.forEach(System.out::println);

        //Read from multitple files
        JavaRDD<String> rdd1 = sparkContext.textFile(ResourceUtils.getURL("classpath:TextFile1.txt").getPath());
        JavaRDD<String> rdd2 = sparkContext.textFile(ResourceUtils.getURL("classpath:TextFile2.txt").getPath());
        JavaRDD<String> rdd3 = sparkContext.textFile(ResourceUtils.getURL("classpath:TextFile3.txt").getPath());

        JavaRDD<String> finalRDD = sparkContext.union(rdd1, rdd2, rdd3);
        List<Tuple2<String, Integer>> result2 = finalRDD.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((c1, c2) -> c1 + c2)
                .collect();

        System.out.println("Word Count for multiple files - TextFile1.txt, TextFile2.txt, TextFile3.txt");
        result2.forEach(System.out::println);
    }
}
