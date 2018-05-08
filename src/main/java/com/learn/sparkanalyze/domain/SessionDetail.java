package com.learn.sparkanalyze.domain;

/**
 * Author: Xukai
 * Description: Session明细
 * CreateDate: 2018/5/4 14:06
 * Modified By:
 */
public class SessionDetail {

    private long taskid;
    private long userid;
    private String sessionid;
    private long pageid;
    private String actionTime;
    private String searchKeyword;
    private long clickCategoryId;
    private long clickProductId;
    private String orderCategoryIds;
    private String orderProductIds;
    private String payCategoryIds;
    private String payProductIds;

    public SessionDetail() {
        super();
    }

    public SessionDetail(long taskid, long userid, String sessionid, long pageid, String actionTime,
                         String searchKeyword, long clickCategoryId, long clickProductId, String orderCategoryIds,
                         String orderProductIds, String payCategoryIds, String payProductIds) {
        this.taskid = taskid;
        this.userid = userid;
        this.sessionid = sessionid;
        this.pageid = pageid;
        this.actionTime = actionTime;
        this.searchKeyword = searchKeyword;
        this.clickCategoryId = clickCategoryId;
        this.clickProductId = clickProductId;
        this.orderCategoryIds = orderCategoryIds;
        this.orderProductIds = orderProductIds;
        this.payCategoryIds = payCategoryIds;
        this.payProductIds = payProductIds;
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getPageid() {
        return pageid;
    }

    public void setPageid(long pageid) {
        this.pageid = pageid;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }
}
