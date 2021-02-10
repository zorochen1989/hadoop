package com.zoro.hbase.util;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * HBASE工具类
 */
public class HBaseUtil {

    /**
     * 新增和更新操作
     *
     * @param table  表
     * @param put    执行新增或更新操作
     * @param family 列簇
     * @param map    数据存放在map中
     */
    public static void saveOrUpdate(Table table, Put put, byte[] family, Map<String, String> map) throws IOException {

        for (String key : map.keySet()) {
            byte[] column = Bytes.toBytes(key);
            byte[] value = Bytes.toBytes(map.get(key));
            put.addColumn(family, column, value);
        }

        table.put(put);

    }
}
