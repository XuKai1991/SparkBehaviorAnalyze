package com.learn.sparkanalyze.dao;

import com.learn.sparkanalyze.domain.Top10Category;

/**
 * Author: Xukai
 * Description: top10热门品类DAO
 * CreateDate: 2018/5/7 16:36
 * Modified By:
 */
public interface ITop10CategoryDAO {

    /*
     * Author: XuKai
     * Description: 插入Top10Category数据
     * Created: 2018/5/7 16:37
     * Params: [top10Category]
     * Returns: void
     */
    void insert(Top10Category top10Category);
}
