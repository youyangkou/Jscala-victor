package com.victor.console.controller;

import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import com.victor.console.domain.RestResponse;
import com.victor.hive.jdbc.QueryBean;
import com.victor.hive.jdbc.QueryManager;
import com.victor.hive.jdbc.QueryState;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@RestController
@RequestMapping("/queryAgent")
public class HiveQueryController {

    @Autowired
    HiveQueryService hiveQueryService;

    @PostMapping("add")
    public RestResponse create(@ApiParam(value = "sql") String sql) {
        QueryBean queryBean = QueryManager.generateQueryBean(null, sql, false);
        HiveQueryBean hiveQueryBean = HiveQueryBean.builder()
                .queryId(queryBean.getQueryId())
                .querySql(queryBean.getSql())
                .project(queryBean.getProject())
                .queryState(QueryState.WAITING.getQueryState())
                .isOnlyQuery(false)
                .tmpTable(queryBean.getTmpTable())
                .updateTime(new Date(System.currentTimeMillis()))
                .log("")
                .build();
        return RestResponse.success(hiveQueryService.add(hiveQueryBean));
    }


    @PostMapping("select")
    public RestResponse select(@ApiParam(value = "query_id") String query_id) {
        return RestResponse.success(hiveQueryService.getHiveQueryBeanById(query_id));
    }


    @PostMapping("delete")
    public RestResponse delete(@ApiParam(value = "query_id") String query_id) {
        HiveQueryBean hiveQueryBeanById = hiveQueryService.getHiveQueryBeanById(query_id);
        hiveQueryService.delete(query_id);
        return RestResponse.success(hiveQueryBeanById);
    }


    @PostMapping("update")
    public RestResponse update(@ApiParam(value = "query_id") String query_id,@ApiParam(value = "query_state") String query_state) {
        HiveQueryBean hiveQueryBean = hiveQueryService.getHiveQueryBeanById(query_id);
        hiveQueryBean.setQueryState(query_state);
        return RestResponse.success( hiveQueryService.update(hiveQueryBean));
    }

}
