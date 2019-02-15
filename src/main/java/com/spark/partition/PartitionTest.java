package com.spark.partition;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class PartitionTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer,String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, "I"),
                new Tuple2(2, "love"),
                new Tuple2(3, "you"),
                new Tuple2(4, "too")
        ), 8);

        // 未使用Hash分区之前
        System.out.println("=== The original parition info ===");
        JavaRDD javaRDD = rdd.mapPartitionsWithIndex((index, iterator) -> {
            System.out.println("index: " + index + ",partition data:" + IteratorUtils.toList(iterator));
            return iterator;
        }, false);

        javaRDD.collect();

        // 使用Hash分区器重新分区
        final JavaPairRDD<Integer, String> rdd2 = rdd.partitionBy(new HashPartitioner(3));
        // 使用Hash分区之后
        System.out.println("=== User HashPartition info ===");
        JavaRDD<Tuple2<Integer, String>> tuple2JavaRDD = rdd2.mapPartitionsWithIndex((index, iterator) -> {
            System.out.println("index: " + index + ",partition data:" + IteratorUtils.toList(iterator));
            return iterator;
        }, false);

        tuple2JavaRDD.collect();

        sc.close();


    }
}
