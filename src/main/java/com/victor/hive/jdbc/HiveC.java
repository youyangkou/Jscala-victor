package com.victor.hive.jdbc;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.parquet.Strings;
import org.mortbay.util.ajax.JSON;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 主要是通过jdbc为程序提供执行入口对象,此处提供关键实现,关键方法为executeQuery
 */
@Slf4j
public class HiveC implements Closeable {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://zjk-al-bigdata-test-00:10000/default";
    private static String user = "root";
    private static String password = "";
    private static Map<SQLBean, HivePreparedStatement> statementMap = new ConcurrentHashMap<>();
    private Connection conn;

    public HiveC() {
    }

    private void mapParams(HivePreparedStatement stmt, List params) throws SQLException {
        if (params == null || params.isEmpty()) return;
        int length = params.size();
        int sqlParameterIndex;
        for (int i = 0; i < length; i++) {

            Object param = params.get(i);
            sqlParameterIndex = i + 1;
            if (param instanceof Number) {
                stmt.setInt(sqlParameterIndex, ((Number) param).intValue());
            } else if (param instanceof String) {
                stmt.setString(sqlParameterIndex, (String) param);
            } else if (param instanceof Boolean) {
                stmt.setBoolean(sqlParameterIndex, (Boolean) param);
            } else {
                StringBuilder errStrBuilder = new StringBuilder();
                errStrBuilder.append("No mapping available for param type: ")
                        .append(param.getClass().getSimpleName())
                        .append(" value: ")
                        .append(param);
                throw new RuntimeException(errStrBuilder.toString());
            }
        }
    }


    public String executeQuery(SQLBean sqlBean) throws ClassNotFoundException, SQLException {
        String resultStr = null;
        if (conn == null) {
            conn = getConn();
        }

        String queryId = sqlBean.getQueryId();
        String sql = sqlBean.getSql();
        List params = sqlBean.getParams();

        HivePreparedStatement stmt;
        synchronized (statementMap) {
            //检查该queryId对应的hql是否正在执行
            this.checkRunningQueue(queryId);
            stmt = (HivePreparedStatement) conn.prepareStatement(sql);
            if (!Strings.isNullOrEmpty(queryId)) {
                //对于提供了queryId的hql,缓存其hivePrepareStatement对象,以提供cancel的功能
                statementMap.put(sqlBean, stmt);
            }
        }

        try {
            //动态匹配PrepareStatement的参数
            mapParams(stmt, params);
            if (stmt.execute()) resultStr = parseResultSet(stmt.getResultSet());
            return resultStr;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (!Strings.isNullOrEmpty(queryId) && statementMap.containsKey(sqlBean)) {
                statementMap.remove(sqlBean);
            }
        }
    }


    private static Set<SQLBean> getSQLBeansInRunningQueueByQueyId(String queryId) {
        return Sets.filter(statementMap.keySet(), new Predicate<SQLBean>() {
            @Override
            public boolean apply(@Nullable SQLBean sqlBean) {
                assert sqlBean != null;
                return sqlBean.getQueryId().equals(queryId);
            }
        });
    }

    private void checkRunningQueue(String queryId) {
        if (Strings.isNullOrEmpty(queryId)) return;
        Set<SQLBean> sets = getSQLBeansInRunningQueueByQueyId(queryId);

        if (sets.isEmpty()) return;

        throw new RuntimeException(String.format("sql [%s] has in the running queue,please wait!", queryId));
    }

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

        log.info("resultStr:" + resultStr);
        return resultStr;
    }


    private Connection getConn() throws ClassNotFoundException, SQLException {
        if (conn == null) {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }

    public void close() throws IOException {
        if (null != conn) try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

