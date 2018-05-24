package com.learn.sparkanalyze.dao;

import com.learn.sparkanalyze.domain.AreaTop3Product;

import java.util.List;

/**
 * Author: Xukai
 * Description: 各区域top3热门商品DAO接口
 * CreateDate: 2018/5/24 11:30
 * Modified By:
 */
public interface IAreaTop3ProductDAO {
    void insert(List<AreaTop3Product> areaTop3Products);
}
