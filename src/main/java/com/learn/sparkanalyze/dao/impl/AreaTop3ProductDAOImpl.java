package com.learn.sparkanalyze.dao.impl;

import com.learn.sparkanalyze.dao.IAreaTop3ProductDAO;
import com.learn.sparkanalyze.domain.AreaTop3Product;
import com.learn.sparkanalyze.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: 各区域top3热门商品DAO实现类
 * CreateDate: 2018/5/24 11:31
 * Modified By:
 */
public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {
    @Override
    public void insert(List<AreaTop3Product> areaTop3Products) {
        String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
        ArrayList<Object[]> paramsList = new ArrayList<>();
        for (AreaTop3Product areaTop3Product : areaTop3Products) {
            Object[] params = new Object[]{
                    areaTop3Product.getTaskid(),
                    areaTop3Product.getArea(),
                    areaTop3Product.getAreaLevel(),
                    areaTop3Product.getProductid(),
                    areaTop3Product.getCityInfos(),
                    areaTop3Product.getClickCount(),
                    areaTop3Product.getProductName(),
                    areaTop3Product.getProductStatus()
            };
            paramsList.add(params);
        }
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }
}
