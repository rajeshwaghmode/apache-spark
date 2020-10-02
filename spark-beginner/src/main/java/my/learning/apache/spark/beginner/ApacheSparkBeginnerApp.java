package my.learning.apache.spark.beginner;

import my.learning.apache.spark.beginner.tutorials.TextFileWordCount;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheSparkBeginnerApp implements CommandLineRunner {
    public static void main(String... args){
        SpringApplication.run(ApacheSparkBeginnerApp.class);
        System.out.println("Running spark-beginner module...");
    }

    @Override
    public void run(String... args) throws Exception {
        TextFileWordCount.main(args);
    }
}
