package com.learn.sparkanalyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.dao.ITaskDAO;
import com.learn.sparkanalyze.dao.factory.DAOFactory;
import com.learn.sparkanalyze.domain.Task;
import com.learn.sparkanalyze.util.ParamUtils;
import com.learn.sparkanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.Date;

/**
 * Author: Xukai
 * Description: 各区域top3热门商品统计Spark作业
 * CreateDate: 2018/5/23 1:04
 * Modified By:
 */
public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSqlContext(sc);

        // 生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

    }


}


















