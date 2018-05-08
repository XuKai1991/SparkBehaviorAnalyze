package com.learn.sparkanalyze.test;

import java.util.Random;

/**
 * Author: Xukai
 * Description:
 * CreateDate: 2018/5/2 14:06
 * Modified By:
 */
public class Test {

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0;i< 100;i++)
        System.out.println(random.nextInt(10));
    }
}
