package com.victor.hive.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.*;
import java.util.List;

/**
 * @author Gerry
 * @date 2022-08-02
 */
@Slf4j
public class HiveJdbcDemo {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://zjk-al-bigdata-test-00:10000/default", "root", "");
        HiveStatement stmt = (HiveStatement) con.createStatement();

        String tableName = "test_hive_driver_table";
//        stmt.execute("drop table if exists " + tableName);

        //tmp_table ddl
        /*String ddl = "create table " + tableName + " as select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720'";
        stmt.execute(ddl);
        System.out.println(ddl);
        getExecterLog(stmt);*/

        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // describe table
        /*sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        getExecterLog(stmt);*/

        // select * query
        /*sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }*/

        // regular hive query
//        sql = "select count(1) from db_real_sync_odps where tb='cdc_sync' and ds='20220720'";
        sql = "create table gerry_test as select count(1) from db_real_sync_odps";
//        sql = "select count(1) from db_real_sync_odps";
        System.out.println("Running: " + sql);
        //executeAsync 提交到远端异步执行，不会一直等待
        //sql不规范会抛出异常
        //没有返回值，会返回false
        boolean exeRes = stmt.execute(sql);
        System.out.println("================================1111111111111====================================================");
        getExecterLog(stmt, true);
        System.out.println("================================22222222222222====================================================");

        //没有返回值
        if (!exeRes) {
            getExecterLog(stmt, true);
        }
        System.out.println("==========================33333333333333==========================================================");

        Thread.sleep(2000);
        if (exeRes) {
            getExecterLog(stmt, true);
            res = stmt.getResultSet();
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            stmt.close();
        }

        getExecterLog(stmt, true);
        System.out.println("=========================4444444444444444==========================================================");
        System.out.println("stmt是否关闭：" + stmt.isClosed());

    }


    public static void getExecterLog(HiveStatement statement, boolean incremental) throws SQLException {
        List<String> queryLog = statement.getQueryLog(incremental, 100);
        for (String logItem : queryLog) {
            System.out.println(logItem);
        }
    }


}
