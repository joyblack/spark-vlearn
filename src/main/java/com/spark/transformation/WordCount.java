package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(WordCount.class);
        SparkConf sparkConf = new SparkConf().setAppName("wordCount").set("spark.testing.memory", "2147480000").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 分割出单词
        JavaRDD<String> wordsRDD = sc.textFile(args[0]).flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 映射为单词对
        JavaPairRDD<String, Integer> wordsPairRDD = wordsRDD.mapToPair(w -> new Tuple2<>(w, 1));

        // 合并统计单词数量
        JavaPairRDD<String, Integer> resultRDD = wordsPairRDD.reduceByKey((x, y) -> x + y);



        // 保存数据
        resultRDD.saveAsTextFile("out");

        logger.info("success, result is save in " + args[1]);
        sc.close();
    }
}
