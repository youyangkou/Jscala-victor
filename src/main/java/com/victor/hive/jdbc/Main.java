package com.victor.hive.jdbc;

/**
 * @author Gerry
 * @date 2022-08-02
 */
public class Main {
    public static void main(String[] args) {
        SQLManager sqlManager = new SQLManager();

//        String sql1 = "drop table if exists gerry_test";
//        String sql2 = "create table gerry_test as select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720'";
        String sql2 = "select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720' limit 10";

//        SQLBean sqlBean1 = SQLBean.builder()
//                .queryId(UUID.randomUUID().toString())
//                .sql(sql1)
//                .params(null)
//                .build();
//        sqlManager.addSqlBeanToPendingQueue(sqlBean1);


        String sqlTrim=sql2.toLowerCase().replaceAll(" ","").trim().hashCode()+"";
        System.out.println(sqlTrim);

        SQLBean sqlBea2 = SQLBean.builder()
                .queryId(sqlTrim)
                .sql(sql2)
                .params(null)
                .build();
        sqlManager.addSqlBeanToPendingQueue(sqlBea2);

        sqlManager.start();

        SQLBean sqlBea3 = SQLBean.builder()
                .queryId(sql2.toLowerCase().replaceAll(" ","").trim().hashCode()+"")
                .sql(sql2)
                .params(null)
                .build();
        sqlManager.addSqlBeanToPendingQueue(sqlBea3);


    }
}
