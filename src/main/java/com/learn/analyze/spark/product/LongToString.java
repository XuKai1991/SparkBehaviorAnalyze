package com.learn.analyze.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Author: Xukai
 * Description: long_to_string
 * CreateDate: 2018/5/24 20:55
 * Modified By:
 */
public class LongToString implements UDF1<Long, String> {
    @Override
    public String call(Long value) throws Exception {
        return String.valueOf(value);
    }
}
