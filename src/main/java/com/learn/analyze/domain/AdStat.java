package com.learn.analyze.domain;

/**
 * Author: Xukai
 * Description: 广告实时统计
 * CreateDate: 2018/5/28 10:22
 * Modified By:
 */
public class AdStat {

    private String date;
    private String province;
    private String city;
    private long adid;
    private long clickCount;

    public AdStat() {
        super();
    }

    public AdStat(String date, String province, String city, long adid, long clickCount) {
        this.date = date;
        this.province = province;
        this.city = city;
        this.adid = adid;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
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
