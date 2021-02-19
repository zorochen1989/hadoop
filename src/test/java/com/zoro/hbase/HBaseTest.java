package com.zoro.hbase;

import com.zoro.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;


public class HBaseTest {

    Admin admin;

    Connection connection;

    String tableName = "promise";

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

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor info = new HColumnDescriptor("info");
        HColumnDescriptor work = new HColumnDescriptor("work");
        table.addFamily(info);
        table.addFamily(work);
        admin.createTable(table);

    }

    /**
     * 添加列簇
     *
     * @throws IOException
     */
    @Test
    public void alterFamily() throws IOException {
        // 表名
        TableName t = TableName.valueOf(tableName);
        // 先禁用table
        admin.disableTable(t);

        // 获取HTableDescriptor
        HTableDescriptor table = admin.getTableDescriptor(t);

        // 新增列簇
        HColumnDescriptor hcd = new HColumnDescriptor("work");
        table.addFamily(hcd);
        admin.addColumn(t, hcd);
        admin.enableTableAsync(t);
    }

    /**
     * 新增
     *
     * @throws IOException
     */
    @Test
    public void insert() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put("1".getBytes());
        byte[] columnFamily = Bytes.toBytes("info");

        // provinceCode
        byte[] provinceCode = Bytes.toBytes("provinceCode");
        byte[] provinceCodeValue = Bytes.toBytes("130000");

        // cityCode
        byte[] cityCode = Bytes.toBytes("cityCode");
        byte[] cityCodeValue = Bytes.toBytes("130100");

        // areaCode
        byte[] areaCode = Bytes.toBytes("areaCode");
        byte[] areaCodeValue = Bytes.toBytes("130101");

        // provinceName
        byte[] provinceName = Bytes.toBytes("provinceName");
        byte[] provinceNameValue = Bytes.toBytes("河北省");

        // cityName
        byte[] cityName = Bytes.toBytes("cityName");
        byte[] cityNameValue = Bytes.toBytes("石家庄市");

        // areaName
        byte[] areaName = Bytes.toBytes("areaName");
        byte[] areaNameValue = Bytes.toBytes("藁城区");

        put.addColumn(columnFamily, provinceCode, provinceCodeValue);
        put.addColumn(columnFamily, cityCode, cityCodeValue);
        put.addColumn(columnFamily, areaCode, areaCodeValue);
        put.addColumn(columnFamily, provinceName, provinceNameValue);
        put.addColumn(columnFamily, cityName, cityNameValue);
        put.addColumn(columnFamily, areaName, areaNameValue);

        table.put(put);

    }

    /**
     * 根据rowKey查询
     *
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));


        Get get = new Get("1".getBytes());

        Result result = table.get(get);
        byte[] columnFamily = Bytes.toBytes("info");

        byte[] provinceCode = result.getValue(columnFamily, Bytes.toBytes("provinceCode"));
        byte[] cityCode = result.getValue(columnFamily, Bytes.toBytes("cityCode"));
        byte[] areaCode = result.getValue(columnFamily, Bytes.toBytes("areaCode"));
        byte[] provinceName = result.getValue(columnFamily, Bytes.toBytes("provinceName"));
        byte[] cityName = result.getValue(columnFamily, Bytes.toBytes("cityName"));
        byte[] areaName = result.getValue(columnFamily, Bytes.toBytes("areaName"));
        System.out.println(" provinceCode: " + new String(provinceCode));
        System.out.println(" cityCode: " + new String(cityCode));
        System.out.println(" areaCode: " + new String(areaCode));
        System.out.println(" provinceName: " + new String(provinceName));
        System.out.println(" cityName: " + new String(cityName));
        System.out.println(" areaName: " + new String(areaName));


    }

    /**
     * 列表操作
     *
     * @throws IOException
     */
    @Test
    public void scan() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        ResultScanner info = table.getScanner(Bytes.toBytes("info"));

        for (Result result : info) {
            System.out.println("result:"+result.toString());

            // 列簇-区域
            byte[] family = "info".getBytes();

            // 列簇下的唯一标识-provinceCode
            byte[] provinceCodes = "provinceCode".getBytes();
            byte[] cityCodes = "cityCode".getBytes();
            byte[] areaCodes = "areaCode".getBytes();
            byte[] provinceNames = "provinceName".getBytes();
            byte[] cityNames = "cityName".getBytes();
            byte[] areaNames = "areaName".getBytes();

            byte[] provinceCode = result.getValue(family, provinceCodes);
            byte[] cityCode = result.getValue(family, cityCodes);
            byte[] areaCode = result.getValue(family, areaCodes);
            byte[] provinceName = result.getValue(family, provinceNames);
            byte[] cityName = result.getValue(family, cityNames);
            byte[] areaName = result.getValue(family, areaNames);

            System.out.println(
                    "provinceCode:" + new String(provinceCode) + " # " +
                            "cityCode:" + new String(cityCode) + " # " +
                            "areaCode:" + new String(areaCode) + " # " +
                            "provinceName:" + new String(provinceName) + " # " +
                            "cityName:" + new String(cityName) + " # " +
                            "areaName:" + new String(areaName) + " # "
            );

        }

    }

    /**
     * 更新操作
     */
    @Test
    public void update() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put("3".getBytes());
        byte[] columnFamily = Bytes.toBytes("info");

        // provinceCode
        byte[] provinceCode = Bytes.toBytes("provinceCode");
        byte[] provinceCodeValue = Bytes.toBytes("150000");

        // cityCode
        byte[] cityCode = Bytes.toBytes("cityCode");
        byte[] cityCodeValue = Bytes.toBytes("150100");

        // areaCode
        byte[] areaCode = Bytes.toBytes("areaCode");
        byte[] areaCodeValue = Bytes.toBytes("150101");

        // provinceName
        byte[] provinceName = Bytes.toBytes("provinceName");
        byte[] provinceNameValue = Bytes.toBytes("山东省");

        // cityName
        byte[] cityName = Bytes.toBytes("cityName");
        byte[] cityNameValue = Bytes.toBytes("济南市");

        // areaName
        byte[] areaName = Bytes.toBytes("areaName");
        byte[] areaNameValue = Bytes.toBytes("开发区");

        put.addColumn(columnFamily, provinceCode, provinceCodeValue);
        put.addColumn(columnFamily, cityCode, cityCodeValue);
        put.addColumn(columnFamily, areaCode, areaCodeValue);
        put.addColumn(columnFamily, provinceName, provinceNameValue);
        put.addColumn(columnFamily, cityName, cityNameValue);
        put.addColumn(columnFamily, areaName, areaNameValue);

        table.put(put);

    }

    /**
     * 删除操作
     */
    @Test
    public void del() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete("3".getBytes());
        table.delete(delete);
    }

    /**
     * 新增或者更新
     */
    @Test
    public void saveOrUpdate() throws IOException {

        // 表
        Table table = connection.getTable(TableName.valueOf(tableName));

        // put操作
        String rowKey = "1";
        Put put = new Put(rowKey.getBytes());

        // 列簇
        String family = "work";

        // 数据
        Map<String, String> map = new LinkedHashMap<>();
        map.put("superHot", "3");
        map.put("firstHot", "3");
        map.put("secondHot", "2");
        map.put("rsWork", "2");
        map.put("bpWork", "2");
        map.put("highSpaceWork", "1");
        map.put("hoistingWork", "2");
        map.put("tempElectricity", "3");
        map.put("groundWork", "4");
        map.put("openCircuitWork", "1");
        map.put("productionNum", "2");
        map.put("runNum", "3");
        map.put("stopNum", "4");
        map.put("majorSourceLevel1", "1");
        map.put("majorSourceLevel2", "1");
        map.put("majorSourceLevel3", "1");
        map.put("majorSourceLevel4", "1");
        map.put("parkingInProgressNum", "2");
        map.put("pilotPlantNum", "3");
        map.put("focusMonitoringHazardousNum", "4");
        map.put("checkMaintenanceNum", "1");

        // 执行更新操作
        HBaseUtil.saveOrUpdate(table, put, Bytes.toBytes(family), map);
    }
}
