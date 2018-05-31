package com.learn.analyze.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 *
 * @author Administrator
 */
public class DateUtils {


    // SimpleDateFormat是非线程安全的，在多线程情况下会有问题，在每个线程下得各自new SimpleDateFormat()即可
    // private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    // private static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
    // private static final SimpleDateFormat MINUTE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    private static SimpleDateFormat getTimeFormat(){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    private static SimpleDateFormat getDateFormat(){
        return new SimpleDateFormat("yyyy-MM-dd");
    }
    private static SimpleDateFormat getDateKeyFormat(){
        return new SimpleDateFormat("yyyyMMdd");
    }

    private static SimpleDateFormat getMinuteFormat() {
        return new SimpleDateFormat("yyyyMMddHHmm");
    }

    /*
     * Author: XuKai
     * Description: 判断一个时间是否在另一个时间之前
     * Created: 2018/5/25 11:43
     * Params: [time1, time2]
     * Returns: boolean
     */
    public static boolean before(String time1, String time2) {
        try {
            Date dateTime1 = getTimeFormat().parse(time1);
            Date dateTime2 = getTimeFormat().parse(time2);

            if (dateTime1.before(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = getTimeFormat().parse(time1);
            Date dateTime2 = getTimeFormat().parse(time2);

            if (dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            Date datetime1 = getTimeFormat().parse(time1);
            Date datetime2 = getTimeFormat().parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.valueOf(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return getDateFormat().format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        return getDateFormat().format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     *
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        return getDateFormat().format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        return getTimeFormat().format(date);
    }

    /**
     * 解析时间字符串
     *
     * @param time 时间字符串
     * @return Date
     */
    public static Date parseTime(String time) {
        try {
            return getTimeFormat().parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /*
     * Author: XuKai
     * Description: 格式化日期key
     * Created: 2018/5/25 12:02
     * Params: [date]
     * Returns: java.lang.String
     */
    public static String formatDateKey(Date date) {
        return getDateKeyFormat().format(date);
    }

    /*
     * Author: XuKai
     * Description: 格式化日期key
     * Created: 2018/5/26 0:26
     * Params: [datekey 格式为yyyyMMdd]
     * Returns: java.util.Date
     */
    public static Date parseDateKey(String datekey) {
        try {
            return getDateKeyFormat().parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        return getMinuteFormat().format(date);
    }

}
