package com.learn.sparkanalyze.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * Author: Xukai
 * Description: concat_long_string
 *  将两个字段拼接起来（使用指定的分隔符）
 *  此函数实现UDF3接口，表示有3个输入参数
 * CreateDate: 2018/5/23 11:17
 * Modified By:
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return String.valueOf(v1) + split + v2;
    }
}
