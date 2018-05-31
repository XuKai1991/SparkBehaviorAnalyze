package com.learn.analyze.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Author: Xukai
 * Description: string_to_long
 * CreateDate: 2018/5/24 20:57
 * Modified By:
 */
public class StringToLong implements UDF1<String, Long> {
    @Override
    public Long call(String value) throws Exception {
        return Long.valueOf(value);
    }
}
