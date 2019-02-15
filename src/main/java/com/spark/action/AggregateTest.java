package com.spark.action;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AggregateTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4),2);
        System.out.println(rdd.getNumPartitions());
        rdd.foreachPartition(s -> System.out.println("partition: " + IteratorUtils.toList(s)));
        System.out.println("result is :" + rdd.aggregate(0, (a, b) -> a + b, (a, b) -> a + b));
        System.out.println("result is (use fold):" + rdd.fold(0, (a, b) -> a + b));
        sc.close();
    }
}
