package com.learn.analyze.dao;

import com.learn.analyze.domain.AdClickTrend;

import java.util.List;

/**
 * Author: Xukai
 * Description: 广告点击趋势DAO接口
 * CreateDate: 2018/5/28 17:35
 * Modified By:
 */
public interface IAdClickTrendDAO {

    void updateBatch(List<AdClickTrend> adClickTrends);

}
