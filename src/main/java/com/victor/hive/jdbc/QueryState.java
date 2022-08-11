package com.victor.hive.jdbc;

/**
 * @author Gerry
 * @date 2022-08-11
 */
public enum QueryState {

    WAITING,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED;

    private QueryState() {}
}
