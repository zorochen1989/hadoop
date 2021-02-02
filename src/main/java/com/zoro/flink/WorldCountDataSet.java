package com.zoro.flink;

import com.zoro.flink.util.ExecuteDataUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

public class WorldCountDataSet {

    public static void main(String[] args) throws Exception {

        // 获取运行时环境：批处理的
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        String input = "hdfs://node-1:9000/wordcount/word.txt";
        //String input = "hdfs://192.168.6.161:9000/wordcount/word.txt";

        // 读取数据源
        DataSource<String> text = env.readTextFile(input);

        // 数据处理+聚合
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

        // 删除数据操作所需代码 start
        Row row = new Row(0);
        DataSet<Row> dataSourceDel = env.fromElements(row);
        String sqlDel = "delete from word ";
        // 删除数据操作所需代码 end

        // 构造数据源
        DataSet<Row> dataSource = env.fromCollection(list);
        String sqlSave = "insert into word(w_name,w_count) values(?,?)";

        // 执行数据删除操作
        ExecuteDataUtil.execute(dataSourceDel, sqlDel);
        env.execute();

        Thread.sleep(4000);

        // 执行数据入库操作
        ExecuteDataUtil.execute(dataSource, sqlSave);
        env.execute();
    }
}
