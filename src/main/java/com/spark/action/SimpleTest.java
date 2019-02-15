package com.spark.action;

import com.spark.transformation.AggregateTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class SimpleTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(90, 20, 10, 50, 60, 70, 80, 30, 100, 40));

        System.out.println("*** The rdd elements: " + rdd.collect());
        System.out.println("*** The rdd elements count: " + rdd.count());
        System.out.println("*** The rdd elementst first ele is: " + rdd.first());
        System.out.println("*** The rdd elements take 1 is: " + rdd.take(1));
        System.out.println("*** The rdd elements take 9 is: " + rdd.take(9));
        System.out.println("*** The rdd elements take 9 and sort is: " + rdd.takeOrdered(9));
        sc.close();
    }
}
