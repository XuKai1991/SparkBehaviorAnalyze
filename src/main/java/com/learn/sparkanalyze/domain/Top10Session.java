package com.learn.sparkanalyze.domain;

/**
 * Author: Xukai
 * Description: top10活跃session
 * CreateDate: 2018/5/8 11:58
 * Modified By:
 */
public class Top10Session {

    private long taskid;
    private long categoryid;
    private String sessionid;
    private long clickCount;

    public Top10Session() {
        super();
    }

    public Top10Session(long taskid, long categoryid, String sessionid, long clickCount) {
        this.taskid = taskid;
        this.categoryid = categoryid;
        this.sessionid = sessionid;
        this.clickCount = clickCount;
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getCategoryid() {
        return categoryid;
    }

    public void setCategoryid(long categoryid) {
        this.categoryid = categoryid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

}
