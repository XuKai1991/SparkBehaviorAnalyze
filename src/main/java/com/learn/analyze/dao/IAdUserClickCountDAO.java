package com.learn.analyze.dao;

import com.learn.analyze.domain.AdUserClickCount;

import java.util.List;

/**
 * Author: Xukai
 * Description: 用户广告点击量DAO接口
 * CreateDate: 2018/5/25 13:56
 * Modified By:
 */
public interface IAdUserClickCountDAO {

    /*
     * Author: XuKai
     * Description: 批量更新用户广告点击量
     * Created: 2018/5/25 13:59
     * Params: [adUserClickCountList]
     * Returns: void
     */
    void updateBatch(List<AdUserClickCount> adUserClickCounts);

    /*
     * Author: XuKai
     * Description: 根据多个key查询用户广告点击量
     * Created: 2018/5/25 14:00
     * Params: [date, userid, adid]
     * Returns: int
     */
    int findClickCountByMultiKey(String date, long userid, long adid);
}
