package com.learn.sparkanalyze.test;

import java.util.*;

/**
 * Author: Xukai
 * Description:
 * CreateDate: 2018/5/8 11:16
 * Modified By:
 */
public class TreeMapSort {
    public static void main(String[] args) {

        // 利用TreeMap实现取TopN
        int[] test = {0, 2, 1, 6, 5, 4, 3, 8, 7};
        int topN = 5;
        List<Integer> result = treeMapSortGetTopN(test, topN);
        for (int i : result) {
            System.out.print(i + " ");
        }

        System.out.println();
        System.out.println("************************************************************");

        // TreeMap默认按照Key的升序排序，以下实现TreeMap按照Key的降序排序
        TreeMap<Integer, Integer> descByKey = new TreeMap<Integer, Integer>(new Comparator<Integer>() {
            /*
            * int compare(Object o1, Object o2) 返回一个基本类型的整型，
            * 返回负数表示：o1 小于o2，
            * 返回0 表示：o1和o2相等，
            * 返回正数表示：o1大于o2。
            */
            public int compare(Integer a, Integer b) {
                return b - a;
            }
        });
        descByKey.put(1, 11);
        descByKey.put(2, 22);
        descByKey.put(7, 77);
        descByKey.put(5, 55);
        System.out.println("descMapByKey = " + descByKey);

        System.out.println("************************************************************");
        // TreeMap默认按照Key的升序排序，以下实现TreeMap按照Value的升序排序
        TreeMap<Integer, Integer> byValue = new TreeMap<Integer, Integer>();
        byValue.put(1, 77);
        byValue.put(2, 55);
        byValue.put(7, 22);
        byValue.put(5, 11);
        List<Map.Entry<Integer, Integer>> entryArrayList = new ArrayList<>(byValue.entrySet());
        Collections.sort(entryArrayList, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        for (Map.Entry<Integer, Integer> entry : entryArrayList) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }


    }

    /*
     * Author: XuKai
     * Description: 利用TreeMap实现取TopN
     * Created: 2018/5/8 11:42
     * Params: [test, topN]
     * Returns: java.util.List<java.lang.Integer>
     */
    public static List<Integer> treeMapSortGetTopN(int[] test, int topN) {
        TreeMap<Integer, Integer> treeMap = new TreeMap<>();
        List result = new ArrayList<Integer>();
        for (int i : test) {
            treeMap.put(i, i);
            if (treeMap.size() > topN) {
                treeMap.remove(treeMap.lastKey());
            }
        }
        while (treeMap.size() != 0) {
            result.add(treeMap.pollFirstEntry().getKey());
        }
        return result;
    }
}
