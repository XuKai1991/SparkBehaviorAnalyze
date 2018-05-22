package com.learn.sparkanalyze.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.learn.sparkanalyze.conf.ConfigurationManager;
import com.learn.sparkanalyze.constant.Constants;

/**
 * 参数工具类
 *
 * @author Administrator
 */
public class ParamUtils {

    /*
     * Author: XuKai
     * Description: 从命令行参数中提取任务id
     * Created: 2018/5/21 12:01 
     * Params: [args 命令行参数, taskType 任务类型]
     * Returns: java.lang.Long 任务id
     */
    public static Long getTaskIdFromArgs(String[] args, String taskType) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            return ConfigurationManager.getLong(taskType);
        } else {
            try {
                if (args != null && args.length > 0) {
                    return Long.valueOf(args[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    /**
     * 从JSON对象中提取参数
     *
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }

}
