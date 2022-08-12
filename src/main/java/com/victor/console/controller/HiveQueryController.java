package com.victor.console.controller;

import com.victor.console.service.HiveQueryService;
import com.victor.domain.RestResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@RestController
@RequestMapping("agent")
public class HiveQueryController {

    @Autowired
    HiveQueryService hiveQueryService;

    @PostMapping("add")
    public RestResponse create(String sql) {

        return null;
    }

}
