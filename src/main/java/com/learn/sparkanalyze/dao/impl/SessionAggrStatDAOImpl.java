package com.learn.sparkanalyze.dao.impl;

import com.learn.sparkanalyze.dao.ISessionAggrStatDAO;
import com.learn.sparkanalyze.domain.SessionAggrStat;
import com.learn.sparkanalyze.jdbc.JDBCHelper;

/**
 * Author: Xukai
 * Description: session聚合统计DAO实现类
 * CreateDate: 2018/5/3 11:02
 * Modified By:
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {

    /*
     * Author: XuKai
     * Description: 将sessionAggrStat插入数据库
     * Created: 2018/5/3 11:08
     * Params: [sessionAggrStat]
     * Returns: void
     */
    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionAggrStat.getTaskid(),
                sessionAggrStat.getSession_count(),
                sessionAggrStat.getVisit_length_1s_3s_ratio(),
                sessionAggrStat.getVisit_length_4s_6s_ratio(),
                sessionAggrStat.getVisit_length_7s_9s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_30s_60s_ratio(),
                sessionAggrStat.getVisit_length_1m_3m_ratio(),
                sessionAggrStat.getVisit_length_3m_10m_ratio(),
                sessionAggrStat.getVisit_length_10m_30m_ratio(),
                sessionAggrStat.getVisit_length_30m_ratio(),
                sessionAggrStat.getStep_length_1_3_ratio(),
                sessionAggrStat.getStep_length_4_6_ratio(),
                sessionAggrStat.getStep_length_7_9_ratio(),
                sessionAggrStat.getStep_length_10_30_ratio(),
                sessionAggrStat.getStep_length_30_60_ratio(),
                sessionAggrStat.getStep_length_60_ratio()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
