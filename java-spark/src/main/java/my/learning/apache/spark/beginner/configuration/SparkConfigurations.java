package my.learning.apache.spark.beginner.configuration;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfigurations {

    @Bean
    public SparkSession sparkSession(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Apache Spark LMS")
                .master("local")
                .getOrCreate();
        System.out.println("Spark Session created.");
        return spark;
    }
}
