package com.victor.hive.jdbc;


/**
 * @author Gerry
 * @date 2022-08-02
 */
public class Main {
    public static void main(String[] args) {
        QueryManager queryManager = new QueryManager();

        String sql = "select count(1) from db_real_sync_odps where tb='cdc_sync' and ds='20220720';";

        QueryBean queryBean1 = queryManager.generateQueryBean(null, sql, true);
        QueryBean queryBean2 = queryManager.generateQueryBean(null, sql, false);

        queryManager.addQueryBeanToPendingQueue(queryBean1);
        queryManager.addQueryBeanToPendingQueue(queryBean2);

        queryManager.start();


    }
}
