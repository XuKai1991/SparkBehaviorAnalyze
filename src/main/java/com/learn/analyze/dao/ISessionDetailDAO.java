package com.learn.analyze.dao;

import com.learn.analyze.domain.SessionDetail;

import java.util.List;

/**
 * Author: Xukai
 * Description: Session明细DAO接口
 * CreateDate: 2018/5/4 14:30
 * Modified By:
 */
public interface ISessionDetailDAO {

    /*
     * Author: XuKai
     * Description: 插入一条session明细数据
     * Created: 2018/5/4 14:30
     * Params: [sessionDetail]
     * Returns: void
     */
    void insert(SessionDetail sessionDetail);

    /*
     * Author: XuKai
     * Description: 批量插入session明细数据
     * Created: 2018/5/4 14:31
     * Params: [sessionDetails]
     * Returns: void
     */
    void insertBatch(List<SessionDetail> sessionDetails);

    /*
     * Author: XuKai
     * Description: 判断session是否已存在表中
     * Created: 2018/5/8 14:04
     * Params: [sessionid]
     * Returns: boolean
     */
    boolean isExist(SessionDetail sessionDetail);
}
