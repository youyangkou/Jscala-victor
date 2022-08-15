package com.victor.console.agent;


import java.sql.SQLException;
import java.util.Scanner;

/**
 * @author Gerry
 * @date 2022-08-02
 */
public class Main {
    public static void main(String[] args) throws SQLException {
        QueryManager queryManager = new QueryManager();
        queryManager.start();

        String sql = "select count(1) from db_real_sync_odps where tb='cdc_sync' and ds='20220720';";

        QueryInstance queryInstance1 = queryManager.generateQueryBean(null, sql, true);
        QueryInstance queryInstance2 = queryManager.generateQueryBean(null, sql, false);

        queryManager.addQueryBeanToPendingQueue(queryInstance1);
        queryManager.addQueryBeanToPendingQueue(queryInstance2);


        System.out.println("开始输入查询语句");
        Scanner sc = new Scanner(System.in);
        String inputSql = sc.nextLine();

        QueryInstance queryInstance = queryManager.generateQueryBean(null, inputSql, false);
        queryManager.addQueryBeanToPendingQueue(queryInstance);

        System.out.println("是否要停止查询");
        boolean isCancel = sc.nextBoolean();
        if (isCancel) {
            if (queryInstance.queryState == QueryState.RUNNING) {
                queryManager.cancelQuery(queryInstance);
            }
        }

        System.out.println("获取查询状态");
        boolean isState = sc.nextBoolean();
        if (isState) {
            System.out.println("查询状态为：" + queryInstance.queryState);
        }


    }
}
