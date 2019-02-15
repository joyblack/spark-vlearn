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

public class MyPartitionerTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String,String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2("a.1", "I"),
                new Tuple2("b.2", "love"),
                new Tuple2("c.3", "you"),
                new Tuple2("d.3", "too")
        ), 8);

        // 未使用Hash分区之前
        System.out.println("=== The original parition info ===");
        JavaRDD javaRDD = rdd.mapPartitionsWithIndex((index, iterator) -> {
            System.out.println("index: " + index + ",partition data:" + IteratorUtils.toList(iterator));
            return iterator;
        }, false);

        javaRDD.collect();

        // 使用Hash分区器重新分区
        final JavaPairRDD<String, String> rdd2 = rdd.partitionBy(new HashPartitioner(3));
        // 使用Hash分区之后
        System.out.println("=== Use HashPartition info ===");
        JavaRDD<Tuple2<String, String>> tuple2JavaRDD = rdd2.mapPartitionsWithIndex((index, iterator) -> {
            System.out.println("index: " + index + ",partition data:" + IteratorUtils.toList(iterator));
            return iterator;
        }, false);

        tuple2JavaRDD.collect();

        // 使用自定义分区器分区
        System.out.println("=== Use MyPartition info ===");
        final JavaPairRDD<String, String> rdd3 = rdd.partitionBy(new MyPartitioner(3));
        JavaRDD<Tuple2<String, String>> tuple3JavaRDD = rdd3.mapPartitionsWithIndex((index, iterator) -> {
            System.out.println("index: " + index + ",partition data:" + IteratorUtils.toList(iterator));
            return iterator;
        }, false);
        tuple3JavaRDD.collect();
        sc.close();


    }
}
