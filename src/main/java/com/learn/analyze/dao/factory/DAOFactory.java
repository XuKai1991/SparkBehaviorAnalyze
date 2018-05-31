package com.learn.analyze.dao.factory;

import com.learn.analyze.dao.*;
import com.learn.analyze.dao.impl.*;

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

    /*
     * Author: XuKai
     * Description: 页面切片转换率
     * Created: 2018/5/21 16:54
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IPageSplitConvertRateDAO
     */
    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 各区域top3热门商品DAO
     * Created: 2018/5/24 11:57
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAreaTop3ProductDAO
     */
    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 用户广告点击量
     * Created: 2018/5/26 0:19
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAdUserClickCountDAO
     */
    public static IAdUserClickCountDAO getAdUserClickCountDAO() {
        return new AdUserClickCountDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 广告黑名单
     * Created: 2018/5/26 18:59
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAdBlacklistDAO
     */
    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 广告实时统计
     * Created: 2018/5/28 11:23
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAdStatDAO
     */
    public static IAdStatDAO getAdStatDAO() {
    	return new AdStatDAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 各省份top3热门广告
     * Created: 2018/5/28 14:17
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAdProvinceTop3DAO
     */
    public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
    	return new AdProvinceTop3DAOImpl();
    }

    /*
     * Author: XuKai
     * Description: 广告点击趋势
     * Created: 2018/5/28 17:40
     * Params: []
     * Returns: com.learn.sparkanalyze.dao.IAdClickTrendDAO
     */
    public static IAdClickTrendDAO getAdClickTrendDAO() {
    	return new AdClickTrendDAOImpl();
    }

}
