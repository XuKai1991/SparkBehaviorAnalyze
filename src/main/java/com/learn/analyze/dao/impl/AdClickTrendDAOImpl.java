package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.IAdClickTrendDAO;
import com.learn.analyze.domain.AdClickTrend;
import com.learn.analyze.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: 广告点击趋势DAO接口实现类
 * CreateDate: 2018/5/28 17:36
 * Modified By:
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 区分出来哪些数据是要插入的，哪些数据是要更新的
        // 提醒一下，比如说，通常来说，同一个key的数据（比如rdd，包含了多条相同的key）
        // 通常在一个分区内一般不会出现重复插入的

        // 但是根据业务需求来
        // 各位自己在实际做项目的时候，一定要自己思考，不要生搬硬套
        // 如果说可能会出现key重复插入的情况
        // 给一个create_time字段

        // j2ee系统在查询的时候，直接查询最新的数据即可（规避掉重复插入的问题）

        // 首先进行分类，分成待插入的和待更新的
        final List<AdClickTrend> insertAdClickTrends = new ArrayList<>();
        final List<AdClickTrend> updateAdClickTrends = new ArrayList<>();

        String searchSql = "select count(*) from ad_click_trend where date=? and hour=? and minute=? and ad_id=?";
        Object[] params = null;
        for (AdClickTrend adClickTrend : adClickTrends) {
            params = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid()
            };
            jdbcHelper.executeQuery(searchSql, params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        if (count > 0) {
                            updateAdClickTrends.add(adClickTrend);
                        } else {
                            insertAdClickTrends.add(adClickTrend);
                        }
                    }
                }
            });
        }

        // 执行批量插入
        String insertSql = "insert into ad_click_trend values(?,?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdClickTrend adClickTrend : insertAdClickTrends) {
            Object[] insertParams = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid(),
                    adClickTrend.getClickCount()
            };
            insertParamsList.add(insertParams);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);

        // 执行批量更新
        String updateSql = "update ad_click_trend set click_count=? where date=? and hour=? and minute=? and ad_id=?";
        List<Object[]> updateParamsList = new ArrayList<>();
        for (AdClickTrend adClickTrend : updateAdClickTrends) {
            Object[] updateParams = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid()
            };
            updateParamsList.add(updateParams);
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);
    }
}
