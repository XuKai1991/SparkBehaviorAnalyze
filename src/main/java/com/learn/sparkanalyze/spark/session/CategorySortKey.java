package com.learn.sparkanalyze.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Author: Xukai
 * Description: 品类二次排序key
 * 封装要进行排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 * 跟其他key相比，如何来判定大于、大于等于、小于、小于等于
 * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
 * （自定义的二次排序key，必须要实现Serializable接口，表明是可以序列化的，否则会报错）
 * CreateDate: 2018/5/7 15:53
 * Modified By:
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (clickCount != that.getClickCount()) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount != that.getOrderCount()) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount != that.getPayCount()) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        return compare(that);
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount > other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount > other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount > other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        return $greater(other);
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if (clickCount < other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        return $less(other);
    }
}
