package com.learn.analyze.dao;

import com.learn.analyze.domain.AreaTop3Product;

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
