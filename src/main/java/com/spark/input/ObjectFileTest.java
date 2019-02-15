package com.spark.input;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class ObjectFileTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<User> rdd = sc.parallelize(Arrays.asList(new User(1,"zhaoyi"),new User(2,"hongqun")));
        rdd.saveAsObjectFile("out_user");
        JavaRDD<User> out = sc.objectFile("out_user");
        out.foreach(s -> System.out.println(s));


    }
}
