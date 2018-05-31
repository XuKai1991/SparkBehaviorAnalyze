package com.learn.analyze.dao;

import com.learn.analyze.domain.SessionAggrStat;

/**
 * Author: Xukai
 * Description:
 * CreateDate: 2018/5/3 10:58
 * Modified By:
 */
public interface ISessionAggrStatDAO {

    /*
     * Author: XuKai
     * Description: 将sessionAggrStat插入数据库
     * Created: 2018/5/3 11:02 
     * Params: [sessionAggrStat]
     * Returns: void
     */
    void insert(SessionAggrStat sessionAggrStat);
}
