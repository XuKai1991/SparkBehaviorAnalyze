package com.learn.sparkanalyze.domain;

/**
 * Author: Xukai
 * Description: 随机抽取的session
 * CreateDate: 2018/5/4 10:57
 * Modified By:
 */
public class SessionRandomExtract {

    private long taskid;
    private String sessionid;
    private String startTime;
    private String searchKeywords;
    private String clickCategoryIds;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }

    public SessionRandomExtract() {
        super();
    }

    public SessionRandomExtract(long taskid, String sessionid, String startTime, String searchKeywords, String clickCategoryIds) {
        this.taskid = taskid;
        this.sessionid = sessionid;
        this.startTime = startTime;
        this.searchKeywords = searchKeywords;
        this.clickCategoryIds = clickCategoryIds;
    }
}
