package com.learn.analyze.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Author: Xukai
 * Description: 给字段加随机数前缀  random_prefix
 * CreateDate: 2018/5/24 13:32
 * Modified By:
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    @Override
    public String call(String key, Integer num) throws Exception {
        Random random = new Random();
        int prefix = random.nextInt(num);
        return prefix + "_" + key;
    }
}
