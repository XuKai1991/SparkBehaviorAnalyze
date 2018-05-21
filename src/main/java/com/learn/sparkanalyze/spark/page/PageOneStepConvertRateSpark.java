package com.learn.sparkanalyze.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.dao.ITaskDAO;
import com.learn.sparkanalyze.dao.factory.DAOFactory;
import com.learn.sparkanalyze.domain.Task;
import com.learn.sparkanalyze.util.ParamUtils;
import com.learn.sparkanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Date;

/**
 * Author: Xukai
 * Description: 页面单跳转化率模块spark作业
 * CreateDate: 2018/5/21 1:10
 * Modified By:
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {
        // 1、构造Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSqlContext(sc.sc());

        // 2、生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3、查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        // 创建需要使用的DAO组件
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 4、查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRdd = SparkUtils.getActionRddByDateRange(sqlContext, taskParam);
    }


}
