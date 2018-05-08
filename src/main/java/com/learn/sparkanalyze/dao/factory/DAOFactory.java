package com.learn.sparkanalyze.dao.factory;

import com.learn.sparkanalyze.dao.*;
import com.learn.sparkanalyze.dao.impl.*;

/**
 * Author: Xukai
 * Description: DAO工厂类
 * CreateDate: 2018/5/3 11:02
 * Modified By:
 */
public class DAOFactory {

    /*
     * Author: XuKai
     * Description: 获取任务管理DAO
     * Created: 2018/5/3 11:17 
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ITaskDAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 获取session聚合统计DAO
     * Created: 2018/5/3 11:19
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ISessionAggrStatDAO
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: session随机抽取DAO
     * Created: 2018/5/4 11:18 
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ISessionRandomExtractDAO
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: Session明细DAO
     * Created: 2018/5/4 14:41
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ISessionDetailDAO
     */
    public static ISessionDetailDAO getSessionDetailDAO() {
    	return new SessionDetailDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: top10热门品类DAO
     * Created: 2018/5/7 17:05
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ITop10CategoryDAO
     */
    public static ITop10CategoryDAO getTop10CategoryDAO() {
    	return new Top10CategoryDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: top10热门Session DAO
     * Created: 2018/5/8 13:04
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.ITop10SessionDAO
     */
    public static ITop10SessionDAO getTop10SessionDAO() {
    	return new Top10SessionDAOImpl();
    }
    //
    // public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
    // 	return new PageSplitConvertRateDAOImpl();
    // }
    //
    // public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
    // 	return new AreaTop3ProductDAOImpl();
    // }
    //
    // public static IAdUserClickCountDAO getAdUserClickCountDAO() {
    // 	return new AdUserClickCountDAOImpl();
    // }
    //
    // public static IAdBlacklistDAO getAdBlacklistDAO() {
    // 	return new AdBlacklistDAOImpl();
    // }
    //
    // public static IAdStatDAO getAdStatDAO() {
    // 	return new AdStatDAOImpl();
    // }
    //
    // public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
    // 	return new AdProvinceTop3DAOImpl();
    // }
    //
    // public static IAdClickTrendDAO getAdClickTrendDAO() {
    // 	return new AdClickTrendDAOImpl();
    // }

}
