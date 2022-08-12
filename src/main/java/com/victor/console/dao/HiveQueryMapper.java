package com.victor.console.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.victor.console.entity.HiveQueryBean;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Mapper
public interface HiveQueryMapper extends BaseMapper<HiveQueryBean> {

    void addHiveQueryBean(HiveQueryBean hiveQueryBean);

    void updateHiveQueryBean(HiveQueryBean hiveQueryBean);

    void deleteHiveQueryBean(String queryId);

}
