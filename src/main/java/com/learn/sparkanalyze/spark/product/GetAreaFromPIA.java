package com.learn.sparkanalyze.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Author: Xukai
 * Description: get_area
 * CreateDate: 2018/5/24 14:59
 * Modified By:
 */
public class GetAreaFromPIA implements UDF1<String, String> {
    @Override
    public String call(String mixedKey) throws Exception {
        return mixedKey.split(":")[1];
    }
}
