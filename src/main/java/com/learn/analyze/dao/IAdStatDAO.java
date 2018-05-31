package com.learn.analyze.dao;

import com.learn.analyze.domain.AdStat;

import java.util.List;

/**
 * Author: Xukai
 * Description: 广告实时统计DAO接口
 * CreateDate: 2018/5/28 10:36
 * Modified By:
 */
public interface IAdStatDAO {

    void updateBatch(List<AdStat> adStats);

}
