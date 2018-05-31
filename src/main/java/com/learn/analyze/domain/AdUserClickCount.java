package com.learn.analyze.domain;

/**
 * Author: Xukai
 * Description: 用户广告点击量
 * CreateDate: 2018/5/25 13:57
 * Modified By:
 */
public class AdUserClickCount {

    private String date;
    private long userid;
    private long adid;
    private long clickCount;

    public AdUserClickCount() {
        super();
    }

    public AdUserClickCount(String date, long userid, long adid, long clickCount) {
        this.date = date;
        this.userid = userid;
        this.adid = adid;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getAdid() {
        return adid;
    }

    public void setAdid(long adid) {
        this.adid = adid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
