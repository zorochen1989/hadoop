package com.zoro.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


public class HBaseTest {

    Admin admin;

    Connection connection;

    String tableName = "userInfo";

    @Before
    public void init() throws IOException {
        // 如果在win上运行，必须加上这句话
        System.setProperty("root", "root");
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @After
    public void over() throws IOException {
        admin.close();
        connection.close();
    }

    @Test
    public void createTable() throws IOException {

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hcd = new HColumnDescriptor("info");

        table.addFamily(hcd);

        admin.createTable(table);

    }

    @Test
    public void insert() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put("1".getBytes());
        byte[] columnFamily = Bytes.toBytes("info");
        // name
        byte[] name = Bytes.toBytes("name");
        byte[] nameValue = Bytes.toBytes("zoro");

        // age
        byte[] age = Bytes.toBytes("age");
        byte[] ageValue = Bytes.toBytes("25");
        put.addColumn(columnFamily, name, nameValue);
        put.addColumn(columnFamily, age, ageValue);
        table.put(put);

    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get("1".getBytes());
        Result result = table.get(get);

        byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
        byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
        System.out.println("name=" + new String(name));
        System.out.println("age=" + new String(age));


    }

    @Test
    public void scan() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        ResultScanner info = table.getScanner(Bytes.toBytes("info"));

        for (Result result : info) {
            // 列簇
            byte[] family = "info".getBytes();
            // 列簇的唯一标识
            byte[] qualifier = "name".getBytes();

            List<Cell> columnCells = result.getColumnCells(family, qualifier);
            columnCells.forEach(cell -> {
                System.out.println("cell:" + cell);
            });
            byte[] value = result.getValue(family, qualifier);
            System.out.println("value:" + new String(value));
            //System.out.println("result:" + new String(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        }

    }
}
