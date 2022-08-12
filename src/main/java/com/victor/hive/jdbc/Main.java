package com.victor.hive.jdbc;


import java.sql.SQLException;
import java.util.Scanner;

/**
 * @author Gerry
 * @date 2022-08-02
 */
public class Main {
    public static void main(String[] args) throws SQLException {
        QueryManager queryManager = new QueryManager();

        String sql = "select count(1) from db_real_sync_odps where tb='cdc_sync' and ds='20220720';";

        QueryBean queryBean1 = queryManager.generateQueryBean(null, sql, true);
        QueryBean queryBean2 = queryManager.generateQueryBean(null, sql, false);

        queryManager.addQueryBeanToPendingQueue(queryBean1);
        queryManager.addQueryBeanToPendingQueue(queryBean2);

        queryManager.start();

        System.out.println("开始输入查询语句");
        Scanner sc = new Scanner(System.in);
        String inputSql = sc.nextLine();

        QueryBean queryBean = queryManager.generateQueryBean(null, inputSql, false);
        queryManager.addQueryBeanToPendingQueue(queryBean);

        System.out.println("是否要停止查询");
        boolean isCancel = sc.nextBoolean();
        if (isCancel) {
            if (queryBean.queryState == QueryState.RUNNING) {
                queryManager.cancelQuery(queryBean);
            }
        }

        System.out.println("获取查询状态");
        boolean isState = sc.nextBoolean();
        if (isState) {
            System.out.println("查询状态为：" + queryBean.queryState);
        }


    }
}
