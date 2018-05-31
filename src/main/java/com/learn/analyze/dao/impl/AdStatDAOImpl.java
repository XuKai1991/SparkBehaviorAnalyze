package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.IAdStatDAO;
import com.learn.analyze.domain.AdStat;
import com.learn.analyze.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description:
 * CreateDate: 2018/5/28 10:36
 * Modified By:
 */
public class AdStatDAOImpl implements IAdStatDAO {

    @Override
    public void updateBatch(List<AdStat> adStats) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        // 首先进行分类，分成待插入的和待更新的
        final ArrayList<AdStat> insertAdStats = new ArrayList<>();
        final ArrayList<AdStat> updateAdStats = new ArrayList<>();

        String searchSql = "select count(*) from ad_stat where date=? and province=? and city=? and ad_id=?";
        Object[] params = null;
        for (AdStat adStat : adStats) {
            params = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()
            };
            jdbcHelper.executeQuery(searchSql, params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        if (count > 0) {
                            updateAdStats.add(adStat);
                        } else {
                            insertAdStats.add(adStat);
                        }
                    }
                }
            });
        }

        // 执行批量插入
        String insertSql = "insert into ad_stat values(?,?,?,?,?)";
        ArrayList<Object[]> paramsList = new ArrayList<>();
        for (AdStat adStat : insertAdStats) {
            paramsList.add(new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid(),
                    adStat.getClickCount()
            });
        }
        jdbcHelper.executeBatch(insertSql, paramsList);

        // 执行批量更新
        String updateSql = "update ad_stat set click_count=click_count+? where date=? and province=? and city=? and ad_id=?";
        List<Object[]> updateParamsList = new ArrayList<>();
        for (AdStat adStat : updateAdStats) {
            updateParamsList.add(new Object[]{
                    adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()
            });
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);


    }
}
