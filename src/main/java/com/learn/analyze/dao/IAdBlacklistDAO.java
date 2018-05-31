package com.learn.analyze.dao;

import java.util.List;

/**
 * Author: Xukai
 * Description: 广告黑名单DAO接口
 * CreateDate: 2018/5/26 18:56
 * Modified By:
 */
public interface IAdBlacklistDAO {

    /*
     * Author: XuKai
     * Description: 批量插入广告黑名单用户
     * Created: 2018/5/26 18:56
     * Params: [adBlacklists]
     * Returns: void
     */
    void insertBatch(List<Long> adBlacklists);

    /*
     * Author: XuKai
     * Description: 查询所有广告黑名单用户
     * Created: 2018/5/26 18:56
     * Params: []
     * Returns: java.util.List<com.learn.sparkanalyze.domain.AdBlacklist>
     */
    List<Long> findAll();
}
