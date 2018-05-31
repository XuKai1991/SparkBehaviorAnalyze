package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.IAdUserClickCountDAO;
import com.learn.analyze.domain.AdUserClickCount;
import com.learn.analyze.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: 用户广告点击量DAO接口实现类
 * CreateDate: 2018/5/25 14:00
 * Modified By:
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        // 首先对用户广告点击量进行分类，分成待插入的和待更新的
        final ArrayList<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
        final ArrayList<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

        String searchSql = "select count(*) from ad_user_click_count where date=? and user_id=? and ad_id=?";
        Object[] params = null;
        for (AdUserClickCount adUserClickCount : adUserClickCounts) {
            params = new Object[]{
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()
            };
            jdbcHelper.executeQuery(searchSql, params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        if (count > 0) {
                            updateAdUserClickCounts.add(adUserClickCount);
                        } else {
                            insertAdUserClickCounts.add(adUserClickCount);
                        }
                    }
                }
            });
        }

        // 执行批量插入
        String insertSql = "insert into ad_user_click_count values(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = new Object[]{
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid(),
                    adUserClickCount.getClickCount()
            };
            insertParamsList.add(insertParams);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);

        // 执行批量更新
        String updateSql = "update ad_user_click_count set click_count=click_count+? where date=? and user_id=? and ad_id=?";
        List<Object[]> updateParamsList = new ArrayList<>();
        for (AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
            Object[] updateParams = new Object[]{
                    adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()
            };
            updateParamsList.add(updateParams);
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);
    }

    @Override
    public int findClickCountByMultiKey(String date, long userid, long adid) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        final int[] clickCount = {0};
        String searchSql = "select click_count from ad_user_click_count where date=? and user_id=? and ad_id=?";
        Object[] searchParams = new Object[]{date, userid, adid};
        jdbcHelper.executeQuery(searchSql, searchParams, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    clickCount[0] = count;
                }
            }
        });
        return clickCount[0];
    }
}
