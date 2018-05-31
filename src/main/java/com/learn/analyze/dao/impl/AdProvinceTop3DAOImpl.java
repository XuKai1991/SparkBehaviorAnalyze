package com.learn.analyze.dao.impl;

import com.learn.analyze.dao.IAdProvinceTop3DAO;
import com.learn.analyze.domain.AdProvinceTop3;
import com.learn.analyze.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Xukai
 * Description: 各省份top3热门广告DAO接口实现类
 * CreateDate: 2018/5/28 14:15
 * Modified By:
 */
public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 先做一次去重（date_province）
        ArrayList<String> dateProvinces = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();
            String dateProvince = date + "_" + province;
            if (!dateProvinces.contains(dateProvince)) {
                dateProvinces.add(dateProvince);
            }
        }

        // 根据去重后的date和province，进行批量删除操作
        String deleteSql = "delete from ad_province_top3 where date=? and province=?";
        ArrayList<Object[]> deleteParams = new ArrayList<>();
        for (String dateProvince : dateProvinces) {
            String[] date_province = dateProvince.split("_");
            deleteParams.add(new Object[]{
                    date_province[0],
                    date_province[1]
            });
        }
        jdbcHelper.executeBatch(deleteSql, deleteParams);

        // 批量插入传入的所有数据
        String insertSql = "insert into ad_province_top3 values(?,?,?,?)";
        ArrayList<Object[]> insertParams = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            insertParams.add(new Object[]{
                    adProvinceTop3.getDate(),
                    adProvinceTop3.getProvince(),
                    adProvinceTop3.getAdid(),
                    adProvinceTop3.getClickCount()
            });
        }
        jdbcHelper.executeBatch(insertSql, insertParams);
    }
}
