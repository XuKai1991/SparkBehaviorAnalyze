package com.learn.sparkanalyze.dao.impl;

import com.learn.sparkanalyze.dao.ISessionDetailDAO;
import com.learn.sparkanalyze.domain.SessionDetail;
import com.learn.sparkanalyze.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: Session明细DAO接口实现类
 * CreateDate: 2018/5/4 14:31
 * Modified By:
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void insertBatch(List<SessionDetail> sessionDetails) {

        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        List<Object[]> paramsList = new ArrayList<Object[]>();
        for (SessionDetail sessionDetail : sessionDetails) {
            Object[] params = new Object[]{
                    sessionDetail.getTaskid(),
                    sessionDetail.getUserid(),
                    sessionDetail.getSessionid(),
                    sessionDetail.getPageid(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickProductId(),
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()
            };
            paramsList.add(params);
        }
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    @Override
    public boolean isExist(SessionDetail sessionDetail) {
        String sql = "select session_id from session_detail where " +
                "task_id=? and user_id=? and session_id=? and page_id=? and action_time=?";
        Object[] params = new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        final boolean[] existFlag = {false};
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    existFlag[0] = true;
                }
            }
        });
        return existFlag[0];
    }
}
