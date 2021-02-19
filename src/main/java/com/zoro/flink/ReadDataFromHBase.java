package com.zoro.flink;

import com.zoro.hbase.reader.HBaseReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 从hbase读取数据
 */
public class ReadDataFromHBase {

    public static void main(String[] args) throws Exception {

        // 获取运行时环境：流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Map<String, String>>> dataStreamSource = env.addSource(new HBaseReader());
        Map<String,Map<String,String>> map = new LinkedHashMap<>();

        dataStreamSource.map(new MapFunction<Tuple2<String, Map<String, String>>, Map<String, Map<String, String>>>() {
            @Override
            public Map<String, Map<String, String>> map(Tuple2<String, Map<String, String>> tuple2) throws Exception {
                map.put(tuple2.f0,tuple2.f1);
                return map;
            }
        }).print();


        env.execute();


    }
}
