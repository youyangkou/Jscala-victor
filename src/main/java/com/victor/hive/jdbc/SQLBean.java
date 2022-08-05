package com.victor.hive.jdbc;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author Gerry
 * @date 2022-08-02
 */
@Data
@Builder
public class SQLBean {
    String queryId;
    String sql;
    List params;
}
