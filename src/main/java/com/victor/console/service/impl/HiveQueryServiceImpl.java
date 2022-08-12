package com.victor.console.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.victor.console.dao.HiveQueryMapper;
import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Service
public class HiveQueryServiceImpl extends ServiceImpl<HiveQueryMapper,HiveQueryBean> implements HiveQueryService{

    @Override
    public HiveQueryBean add(HiveQueryBean hiveQueryBean) {
        return null;
    }
}


