package my.learning.apache.spark.beginner.tutorials.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import scala.Array;

import java.io.FileNotFoundException;

@Component
public class DataFrameSchemaDemo01 {

    @Autowired
    private SparkSession sparkSession;

    public void execute(String... args) throws FileNotFoundException {
        Dataset<Row> dataFrame1 = sparkSession
                .read()
                .json(ResourceUtils.getURL("classpath:data/flight-data/json/2015-summary.json").getPath());

        dataFrame1.printSchema(); //Prints the schema to the console in a nice tree format.

        //Configuring Schema options
        sparkSession
            .read()
            .format("json")
            .option("inferSchema", "true")
//            .option("header", "true")
            .load(ResourceUtils.getURL("classpath:data/flight-data/json/2015-summary.json").getPath())
            .toDF("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME", "count")
            .show(1);

        //Select/ selectExpr
        sparkSession
            .read()
            .csv(ResourceUtils.getURL("classpath:data/flight-data/json/2015-summary.json").getPath())
//            .select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME", "count")
//            .select(dataFrame1.col("DEST_COUNTRY_NAME"), dataFrame1.col("ORIGIN_COUNTRY_NAME"), dataFrame1.col("count"))
            .selectExpr("DEST_COUNTRY_NAME as Destination_Country","ORIGIN_COUNTRY_NAME as Origin_Country", "count")
            .show(2);

    }
}
