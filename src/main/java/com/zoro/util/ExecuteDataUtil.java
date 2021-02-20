package com.zoro.util;

import com.zoro.config.DBConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;

/**
 * Flink操作数据
 */
public class ExecuteDataUtil {

    /**
     * 执行相关SQL
     *
     * @param dataSet
     * @param sql
     */
    public static void execute(DataSet dataSet, String sql) {
        dataSet.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername(DBConfig.ORACLE_DRIVER)
                        .setDBUrl(DBConfig.ORACLE_URL)
                        .setUsername(DBConfig.ORACLE_USER)
                        .setPassword(DBConfig.ORACLE_PWD)
                        .setQuery(sql)
                        .finish());
    }
}
