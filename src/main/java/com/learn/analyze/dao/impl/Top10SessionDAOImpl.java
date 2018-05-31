package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.ITop10SessionDAO;
import com.learn.analyze.domain.Top10Session;
import com.learn.analyze.jdbc.JDBCHelper;

/**
 * Author: Xukai
 * Description: top10热门Session的DAO实现类
 * CreateDate: 2018/5/8 12:14
 * Modified By:
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
