package com.victor.hive.jdbc;

import com.google.common.base.Objects;
import lombok.Builder;
import lombok.Data;
import org.apache.hive.jdbc.HiveStatement;


/**
 * @author Gerry
 * @date 2022-08-02
 */
@Data
@Builder
public class QueryBean {

    /**
     * the hashcode of query
     */
    String queryId;

    /**
     * the HiveStatement of this query
     */
    HiveStatement stmt;

    /**
     * the recieve query sql | the ddl of tmp_table
     */
    String sql;

    /**
     * the db of hive
     */
    String project;

    /**
     * the tmp table
     */
    String tmpTable;

    /**
     * is requeried to create a tmp table
     */
    boolean isOnlyQuery;

    /**
     * the state of query
     */
    QueryState queryState;

    /**
     * the log of query
     */
    String log;


    /**
     * the result of query
     */
    String result;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryBean queryBean = (QueryBean) o;
        return isOnlyQuery == queryBean.isOnlyQuery &&
                Objects.equal(queryId, queryBean.queryId) &&
                Objects.equal(sql, queryBean.sql) &&
                Objects.equal(project, queryBean.project) &&
                Objects.equal(tmpTable, queryBean.tmpTable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryId, sql, project, tmpTable, isOnlyQuery);
    }


    public static void main(String[] args) {
        QueryBean queryBean = QueryBean.builder().build();
        queryBean.queryState = QueryState.SUCCESS;
        System.out.println(queryBean);
    }


}
