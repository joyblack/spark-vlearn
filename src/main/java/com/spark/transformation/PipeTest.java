package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PipeTest {
    public static void main(String[] args) {
        String[] jars = {"C:\\Users\\10160\\eclipse-workspace\\spark-vlearn\\target\\spark-learn-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        Logger logger = LoggerFactory.getLogger(PipeTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("spark://h131:7077").set("spark.testing.memory", "2147480000");
        sparkConf.setJars(jars);
        sparkConf.setIfMissing("spark.driver.host","192.168.103.206");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("My","name","is","joy"));
        JavaRDD<String> result = rdd.pipe("/opt/software/pipe.sh");
        result.collect().forEach(System.out::println);
        sc.close();
    }

}
