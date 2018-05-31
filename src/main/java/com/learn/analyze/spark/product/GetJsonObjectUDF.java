package com.learn.analyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.learn.analyze.util.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

/**
 * Author: Xukai
 * Description: get_json_object()，从字段为json格式的内容中获得数据
 * CreateDate: 2018/5/24 0:49
 * Modified By:
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    @Override
    public String call(String json, String field) throws Exception {
        if (StringUtils.isNotEmpty(json)) {
            JSONObject jsonObject = JSONObject.parseObject(json);
            if (jsonObject.containsKey(field)) {
                return jsonObject.getString(field);
            }
        }
        return null;
    }
}
