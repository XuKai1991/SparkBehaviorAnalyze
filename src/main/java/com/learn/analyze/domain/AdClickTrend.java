package com.learn.analyze.domain;

/**
 * Author: Xukai
 * Description: 广告点击趋势
 * CreateDate: 2018/5/28 17:28
 * Modified By:
 */
public class AdClickTrend {

    private String date;
    private String hour;
    private String minute;
    private long adid;
    private long clickCount;

    public AdClickTrend() {
        super();
    }

    public AdClickTrend(String date, String hour, String minute, long adid, long clickCount) {
        this.date = date;
        this.hour = hour;
        this.minute = minute;
        this.adid = adid;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
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
