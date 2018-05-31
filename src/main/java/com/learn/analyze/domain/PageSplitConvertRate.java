package com.learn.analyze.domain;

/**
 * Author: Xukai
 * Description: 页面切片转化率
 * CreateDate: 2018/5/21 16:47
 * Modified By:
 */
public class PageSplitConvertRate {
    private long taskid;
    private String convertRate;

    public PageSplitConvertRate() {
        super();
    }

    public PageSplitConvertRate(long taskid, String convertRate) {
        this.taskid = taskid;
        this.convertRate = convertRate;
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
