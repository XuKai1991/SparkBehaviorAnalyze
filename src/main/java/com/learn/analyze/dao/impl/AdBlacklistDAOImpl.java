package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.IAdBlacklistDAO;
import com.learn.analyze.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: 广告黑名单DAO接口实现类
 * CreateDate: 2018/5/26 18:57
 * Modified By:
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<Long> adBlacklists) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String insertSql = "insert into ad_blacklist values(?)";
        List<Object[]> params = new ArrayList<Object[]>();
        for (Long userid : adBlacklists) {
            params.add(
                    new Object[]{userid}
            );
        }
        jdbcHelper.executeBatch(insertSql, params);
    }

    @Override
    public List<Long> findAll() {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String searchSql = "select * from ad_blacklist";
        final List<Long> adBlacklists = new ArrayList<Long>();
        jdbcHelper.executeQuery(searchSql, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long userid = Long.valueOf(String.valueOf(rs.getInt(1)));
                    adBlacklists.add(userid);
                }
            }
        });
        return adBlacklists;
    }
}
