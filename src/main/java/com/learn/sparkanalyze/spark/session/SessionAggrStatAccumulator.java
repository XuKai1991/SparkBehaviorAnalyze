package com.learn.sparkanalyze.spark.session;

import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Author: Xukai
 * Description: session聚合统计Accumulator
 * 使用自定义的数据格式，如String，甚至可以自定义model、自定义的类（必须可序列化）
 * 然后可以基于这种特殊的数据格式，实现自己复杂的分布式的计算逻辑
 * 各个task，分布式运行，可以根据需求，task给Accumulator传入不同的值
 * 根据不同的值，做复杂的逻辑
 * 是Spark Core里面很实用的高端技术
 * CreateDate: 2018/5/2 23:45
 * Modified By:
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    /*
     * Author: XuKai
     * Description: zero方法，主要用于数据的初始化
	 * 这里就返回一个值，就是初始化中，所有范围区间的数量，都是0
	 * 各个范围区间的统计数量的拼接，还是采用key=value|key=value的连接串的格式
     * Created: 2018/5/3 0:02
     * Params: [initialValue]
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /*
     * Author: XuKai
     * Description: addInPlace和addAccumulator可以理解为是一样的
	 * 这两个方法，其实主要实现，v1是初始化的连接串
	 * v2是在遍历session时，判断出某个session对应的区间，比如用Constants.TIME_PERIOD_1s_3s = * 传入数据
	 * 具体操作过程为：在v1中，找到v2对应的value，累加1，然后再更新回连接串里面去
     * Created: 2018/5/3 0:04
     * Params: [v1, v2]
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /*
     * Author: XuKai
     * Description: 
     * Created: 2018/5/3 0:24
     * Params: [v1 连接串, v2 范围区间]
     * Returns: java.lang.String 更新以后的连接串
     */
    private String add(String v1, String v2) {
        // 校验：v1为空，直接返回v2
        if(StringUtils.isEmpty(v1)){
            return v2;
        }

        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if(StringUtils.isNotEmpty(oldValue)){
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}
