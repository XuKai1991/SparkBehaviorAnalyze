package com.learn.sparkanalyze.dao;

import com.learn.sparkanalyze.domain.Task;

/**
 * 任务管理DAO接口
 *
 * @author Administrator
 */
public interface ITaskDAO {

    /*
     * Author: XuKai
     * Description: 根据主键查询任务
     * Created: 2018/5/3 10:59
     * Params: [taskid]
     * Returns: com.learn.sparkanalyze.domain.Task
     */
    Task findById(long taskid);

}
