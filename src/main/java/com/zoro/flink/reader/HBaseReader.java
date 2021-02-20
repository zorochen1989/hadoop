package com.zoro.flink.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class HBaseReader extends RichSourceFunction<Tuple2<String, Map<String, String>>> {

    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.setProperty("root", "root");
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("promise"));
        scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addFamily(Bytes.toBytes("work"));

    }

    @Override
    public void run(SourceContext<Tuple2<String, Map<String, String>>> ctx) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            Map<String, String> map = new LinkedHashMap<>();
            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                map.put(qualifier,value);
            }
            Tuple2<String, Map<String, String>> tuple2 = new Tuple2<>();
            tuple2.setFields(rowKey, map);
            ctx.collect(tuple2);

        }

    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            System.out.println("Close HBase Exception:" + e.toString());
        }
    }

}
