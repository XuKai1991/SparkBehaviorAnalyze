package com.learn.analyze.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Author: Xukai
 * Description: get_product_id
 * CreateDate: 2018/5/24 14:54
 * Modified By:
 */
public class GetProductIdFromPIA implements UDF1<String, Long> {
    @Override
    public Long call(String mixedKey) throws Exception {
        String productIdStr = mixedKey.split(":")[0];
        return Long.valueOf(productIdStr);
    }
}
