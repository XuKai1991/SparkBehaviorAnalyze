package com.learn.analyze.dao;

import com.learn.analyze.domain.AdProvinceTop3;

import java.util.List;

/**
 * Author: Xukai
 * Description: 各省份top3热门广告DAO接口
 * CreateDate: 2018/5/28 13:54
 * Modified By:
 */
public interface IAdProvinceTop3DAO {

    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
