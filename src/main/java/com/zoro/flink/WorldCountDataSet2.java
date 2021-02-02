package com.zoro.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

public class WorldCountDataSet2 {

    public static void main(String[] args) throws Exception {

        // 获取运行时环境：批处理的
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        String input = "hdfs://node-1:9000/wordcount/word.txt";
        //String input = "hdfs://192.168.6.161:9000/wordcount/word.txt";

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

        // 数据转换：输出到mysql必须要以Row类型进行
        List<Row> list = new LinkedList<>();
        collect.forEach(model -> {
            System.out.println("key:" + model.f0 + " || value:" + model.f1);
            Row row = new Row(2);
            row.setField(0, model.f0);
            row.setField(1, model.f1);
            list.add(row);
        });

        DataSet<Row> dataSource = env.fromCollection(list);
        dataSource.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://192.168.0.101:3306/db2019")
                        .setUsername("root")
                        .setPassword("root")
                        .setQuery("insert into word(w_name,w_count) values(?,?)")
                        .finish());

        env.execute();
    }
}
