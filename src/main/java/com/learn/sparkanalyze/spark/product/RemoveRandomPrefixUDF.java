package com.learn.sparkanalyze.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Author: Xukai
 * Description: 把字段的随机数前缀去掉  remove_random_prefix
 * CreateDate: 2018/5/24 13:51
 * Modified By:
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
    @Override
    public String call(String key) throws Exception {
        return key.split("_")[1];
    }
}
