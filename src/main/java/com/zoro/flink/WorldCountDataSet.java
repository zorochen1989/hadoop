package com.zoro.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class WorldCountDataSet {

    public static void main(String[] args) throws Exception {

        // 获取运行时环境：批处理的
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String input = "hdfs://node-1:9000/wordcount/word.txt";

        // 读取数据源
        DataSource<String> text = env.readTextFile(input);

        // 数据处理
        List<Tuple2<String, Integer>> collect = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                if (value.length() > 0) {
                    String[] arr = value.split(" ");
                    for (String str : arr) {
                        out.collect(new Tuple2<>(str, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).collect();

        collect.forEach(model -> {
            System.out.println("key:" + model.f0 + " || value:" + model.f1);
        });

    }
}
