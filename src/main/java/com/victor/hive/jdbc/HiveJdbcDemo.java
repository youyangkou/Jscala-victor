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
        boolean isEnd = false;

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://zjk-al-bigdata-test-00:10000/default", "root", "");

        //        HiveStatement stmt = (HiveStatement) con.createStatement();
        /*String tableName = "test_hive_driver_table";
        stmt.execute("drop table if exists " + tableName);

        //tmp_table ddl
        String ddl = "create table " + tableName + " as select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720'";
        stmt.execute(ddl);
        System.out.println(ddl);*/





        // show tables
        HiveStatement stmt1 = (HiveStatement) con.createStatement();
        String sql = "show tables db_real_sync_odps";
        System.out.println("Running: " + sql);
        ResultSet res = stmt1.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }


        // select * query
        /*sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }*/

        // regular hive query
//        sql = "select count(1) from db_real_sync_odps where tb='cdc_sync' and ds='20220720'";
        HiveStatement stmt2 = (HiveStatement) con.createStatement();
        sql = "create table gerry_test as select count(1) from db_real_sync_odps";
        System.out.println("Running: " + sql);
        //executeAsync 提交到远端异步执行，不会一直等待;execute会异步提交到远端执行，而且会一直等待成功才进行下一步
        //sql不规范会抛出异常
        //没有返回值，会返回false
        boolean exeRes = stmt2.executeAsync(sql);
        if (!exeRes) {
            System.out.println("exeRes为false");
            isEnd = getExecterLog(stmt2, true, isEnd);
        } else {
            System.out.println("exeRes为true");
            isEnd = getExecterLog(stmt2, true, isEnd);
            res = stmt2.getResultSet();
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            stmt2.close();
        }

        int i = 0;
        //轮询获取日志
        while (!isEnd) {
            isEnd = getExecterLog(stmt2, true, isEnd);
            i++;
            if (i > 20) {
                isEnd = true;
                stmt2.cancel();
                stmt2.close();
            }
            Thread.sleep(2000);
        }
        System.out.println("stmt是否关闭：" + stmt2.isClosed());
        getExecterLog(stmt1, true, isEnd);
    }



    /**
     * 获取执行日志
     *
     * @param statement   Hive statement
     * @param incremental 是否增量获取
     * @param isEnd       查询是否结束
     * @return 查询是否结束
     * @throws SQLException 非法SQL会抛出异常
     */
    public static boolean getExecterLog(HiveStatement statement, boolean incremental, boolean isEnd) throws SQLException {
        System.out.println("开始打印日志");
        List<String> queryLog = statement.getQueryLog(incremental, 100);
        if (queryLog.size() > 0) {
            for (String logItem : queryLog) {
                System.out.println(logItem);
                if (logItem.contains("INFO  : OK")) {
                    isEnd = true;
                }
            }
        }
        return isEnd;
    }


}
