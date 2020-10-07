package my.learning.apache.spark.beginner.configuration;

import my.learning.apache.spark.beginner.tutorials.dataframe.DataFrameSchemaDemo01;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CommandLineRunnerConfiguration implements CommandLineRunner {
    @Autowired
    DataFrameSchemaDemo01 dataFrameSchemaDemo01;

    @Override
    public void run(String... args) throws Exception {
//        TextFileWordCount1.main(args);
//        TextFileWordCount2.main(args);
        dataFrameSchemaDemo01.execute(args);
    }
}
