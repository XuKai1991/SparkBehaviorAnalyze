package com.learn.sparkanalyze.dao.impl;

import com.learn.sparkanalyze.dao.ITop10CategoryDAO;
import com.learn.sparkanalyze.domain.Top10Category;
import com.learn.sparkanalyze.jdbc.JDBCHelper;

/**
 * Author: Xukai
 * Description: top10热门品类DAO实现类
 * CreateDate: 2018/5/7 16:38
 * Modified By:
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getOrderCount(),
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
