package com.spark.partition;

import org.apache.spark.Partitioner;

public class MyPartitioner extends Partitioner {

    // 分区数量
    private final int partitions;

    // 返回分区数
    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {
        int index;
        if (key == null) {
            index = 0;
        } else {
            String keyString = key.toString();
            // 获取key的最后一个数字
            index = Integer.valueOf(keyString.substring(keyString.length() - 1)) % partitions;
        }
        return index;
    }

    public MyPartitioner(final int partitions) {
        this.partitions = partitions;
    }
}
