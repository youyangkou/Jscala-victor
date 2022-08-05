package com.victor.hive.thrift;

import org.apache.hive.service.rpc.thrift.TOperationHandle;

/**
 * @author Gerry
 * @date 2022-08-02
 */
public class Test {
    public static void main(String[] args) {
        try {
            String ddlSql = "show partitions db_real_sync_odps";
            String querySql = "use default;select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720' limit 10";

            QueryInstance instance = new QueryInstance();
            TOperationHandle handle = instance.submitQuery(ddlSql);

            instance.getResults(handle);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}