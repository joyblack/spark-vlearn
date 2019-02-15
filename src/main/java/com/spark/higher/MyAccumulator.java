package com.spark.higher;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

// IN String
// OUT List<String>
public class MyAccumulator extends AccumulatorV2<String,List<String>> {
    // 创建输出结果类型，对应OUT
    List<String> list = new ArrayList<String>();

    public MyAccumulator() {
        super();
    }

    // 判断当前内存数据是否为空的依据
    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    // 提供spark调用，实现产生一个新的累加器实例
    @Override
    public AccumulatorV2<String, List<String>> copy() {
        MyAccumulator tmp = new MyAccumulator();
        tmp.list.addAll(list);
        return tmp;
    }

    // 重置数据
    @Override
    public void reset() {
        list.clear();
    }

    // 添加数据
    @Override
    public void add(String v) {
        list.add(v);
    }

    // 合并多个分区的累加器实例
    @Override
    public void merge(AccumulatorV2<String,List<String>> other) {
        list.addAll(other.value());
    }

    // 通过value获取累加器的最终结果
    @Override
    public List<String> value() {
        return list;
    }
}
