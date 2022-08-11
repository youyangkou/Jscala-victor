package com.victor.hive.jdbc;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.parquet.Strings;
import org.mortbay.util.ajax.JSON;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Gerry
 * @date 2022-08-08
 */
public class HiveClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://zjk-al-bigdata-test-00:10000/default";
    private static String user = "root";
    private static String password = "";
    private static Map<QueryBean, HiveStatement> statementMap = new ConcurrentHashMap<>();
    private Connection conn;


    /**
     * 执行查询
     *
     * @param queryBean 封装的查询实体
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public QueryBean executeQuery(QueryBean queryBean) throws ClassNotFoundException, SQLException {
        String resultStr = "";
        String queryId = queryBean.getQueryId();
        String sql = queryBean.getSql();

        if (conn == null) {
            conn = getConn();
        }

        HiveStatement stmt;
        synchronized (statementMap) {
            //检查该query是否正在运行
            this.checkRunningQueue(queryId);
            stmt = (HiveStatement) conn.createStatement();
            queryBean.stmt = stmt;

            if (!StringUtils.isEmpty(queryId)) {
                if (queryBean.isOnlyQuery) {
                    try {
                        resultStr = parseResultSet(stmt.executeQuery(sql));
                        queryBean.queryState = QueryState.SUCCESS;
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (!Strings.isNullOrEmpty(queryId) && statementMap.containsKey(queryBean)) {
                            statementMap.remove(queryBean);
                        }
                    }
                } else {
                    //需要异步执行的query,缓存其HiveStatement对象,以提供cancel的功能
                    statementMap.put(queryBean, stmt);
                    stmt.executeAsync(sql);
                    queryBean.queryState = QueryState.RUNNING;
                }
            }
        }
        queryBean.result = resultStr;
        return queryBean;
    }


    /**
     * 取消查询
     *
     * @param queryBean
     * @return
     * @throws SQLException
     */
    public boolean cancelQuery(QueryBean queryBean) throws SQLException {
        String queryId = queryBean.getQueryId();
        boolean isSuccess = false;
        HiveStatement stmt = null;

        synchronized (statementMap) {
            //判断statementMap是否包含queryId
            if (!statementMap.containsKey(queryBean)) {
                throw new RuntimeException(String.format("sql [%s] has been canceled!", queryId));
            } else {
                try {
                    stmt = statementMap.get(queryId);
                    stmt.cancel();
                    isSuccess = true;
                } catch (Exception e) {
                    System.out.println("cancel失败");
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (!Strings.isNullOrEmpty(queryId) && statementMap.containsKey(queryBean)) {
                        statementMap.remove(queryBean);
                    }
                }
            }
        }
        return isSuccess;
    }


    /*
            RUNNING,
            SUCCESS,
            FAILED,
            CANCELLED;
    */
    public String getQueryState(QueryBean queryBean) throws SQLException {
        String queryId = queryBean.getQueryId();
        boolean isSuccess = false;
        HiveStatement stmt = null;
        return null;
    }


    /**
     * 等待异步执行，获取执行日志
     *
     * @param queryBean
     * @return
     * @throws Exception
     */
    public QueryBean waitForOperationToComplete(QueryBean queryBean) throws Exception {
        HiveStatement stmt = queryBean.stmt;
        StringBuilder sb = new StringBuilder();

        boolean isEnd = false;
        int i = 0;
        while (!isEnd) {
            Tuple2<String, Boolean> execterLogBean = getExecterLog(stmt, true, isEnd, sb);
            isEnd = execterLogBean._2;
            sb.append(execterLogBean._1);

            if (isEnd) queryBean.queryState = QueryState.SUCCESS;
            i++;
            if (i > 1000) {
                isEnd = true;
                stmt.cancel();
                stmt.close();
                queryBean.queryState = QueryState.FAILED;
            }
            Thread.sleep(5000);
        }
        queryBean.log = sb.toString();

        return queryBean;
    }


    private Tuple2<String, Boolean> getExecterLog(HiveStatement stmt, boolean incremental, boolean isEnd, StringBuilder sb) throws SQLException {
        List<String> queryLog = stmt.getQueryLog(incremental, 100);
        if (queryLog.size() > 0) {
            for (String logItem : queryLog) {
                sb.append(logItem + "\n");
                if (logItem.contains("INFO  : OK")) {
                    isEnd = true;
                }
            }
        }
        return Tuple2.apply(sb.toString(), isEnd);
    }


    /**
     * 检查该queryId对应的hql是否正在执行
     *
     * @param queryId
     */
    private void checkRunningQueue(String queryId) {
        if (Strings.isNullOrEmpty(queryId)) return;

        Set<QueryBean> sets = getQueryBeansInRunningQueueByQueryId(queryId);
        if (sets.isEmpty()) return;

        throw new RuntimeException(String.format("sql [%s] has in the running queue,please wait!", queryId));
    }


    private static Set<QueryBean> getQueryBeansInRunningQueueByQueryId(String queryId) {
        return Sets.filter(statementMap.keySet(), new Predicate<QueryBean>() {
            @Override
            public boolean apply(@Nullable QueryBean queryBean) {
                assert queryBean != null;
                return queryBean.getQueryId().equals(queryId);
            }
        });
    }


    /**
     * 解析Hive查询返回值，封装为JSON返回
     *
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private String parseResultSet(ResultSet resultSet) throws SQLException {
        String resultStr;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int colCount = metaData.getColumnCount();
        List res = Lists.newArrayList();
        while (resultSet.next()) {
            List row = Lists.newArrayList();
            for (int i = 1; i <= colCount; i++) {
                row.add(resultSet.getObject(i));
            }
            res.addAll(row);
        }
        resultStr = JSON.toString(res);
        return resultStr;
    }


    private Connection getConn() throws ClassNotFoundException, SQLException {
        if (conn == null) {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }


    /**
     * 接口接受到请求之后，判断类型，然后进行封装
     *
     * @param project
     * @param sql
     * @return
     */
    private QueryBean generateQueryBean(String project, String sql, boolean isOnlyQuery) {

        if (StringUtils.isEmpty(project)) {
            project = "default";
        }
        if (StringUtils.isEmpty(sql)) {
            throw new IllegalArgumentException("sql must not be empty");
        }
        if (!sql.toLowerCase().trim().startsWith("select")) {
            throw new IllegalArgumentException(
                    "Prohibit submission of queries that do not start with select");
        }
        if (sql.trim().endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        String queryId = String.valueOf(sql.toLowerCase().replaceAll(" ", "").trim().hashCode());
        String tmpTable = "";

        if (!isOnlyQuery) {
            tmpTable = "tmp_" + UUID.randomUUID().toString().replace("-", "_");

            StringBuilder ddlSQL = new StringBuilder("Create Table ")
                    .append(project)
                    .append(".")
                    .append(tmpTable)
                    .append(" lifecycle 1 as ")
                    .append(sql);

            sql = ddlSQL.toString();
        }

        return QueryBean.builder()
                .project(project)
                .sql(sql)
                .tmpTable(tmpTable)
                .queryId(queryId)
                .isOnlyQuery(isOnlyQuery)
                .build();
    }


}
