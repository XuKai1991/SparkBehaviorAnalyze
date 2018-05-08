package com.learn.sparkanalyze.dao.impl;

import com.learn.sparkanalyze.dao.ISessionRandomExtractDAO;
import com.learn.sparkanalyze.domain.SessionRandomExtract;
import com.learn.sparkanalyze.jdbc.JDBCHelper;

/**
 * Author: Xukai
 * Description: session随机抽取模块DAO接口实现类
 * CreateDate: 2018/5/4 11:08
 * Modified By:
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO{

    /*
     * Author: XuKai
     * Description: 插入session随机抽取
     * Created: 2018/5/4 11:10
     * Params: [sessionRandomExtract]
     * Returns: void
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
