package my.learning.apache.spark.beginner.tutorials;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.util.ResourceUtils;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class TextFileWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String... args) throws FileNotFoundException {
        SparkConf sparkConf = new SparkConf().setAppName("TextFileWordCount").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile(ResourceUtils.getURL("TextFile1.txt").getPath());
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCount = ones.reduceByKey((count1, count2) -> count1 + count2);
//        List<Tuple2<String, Integer>> output = wordCount.collect();
        System.out.println("Word COunt for file TextFile1.txt");
        wordCount.collect().forEach(System.out::println);
    }
}
