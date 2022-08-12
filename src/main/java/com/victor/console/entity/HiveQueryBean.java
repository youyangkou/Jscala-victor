package com.victor.console.entity;

import com.victor.hive.jdbc.QueryState;
import lombok.Builder;
import lombok.Data;
import org.apache.hive.jdbc.HiveStatement;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Data
@Builder
public class HiveQueryBean {
    /**
     * the hashcode of query
     */
    String queryId;

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
    String queryState;

    /**
     * the log of query
     */
    String log;

}
