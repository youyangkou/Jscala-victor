package com.victor.jdk;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Scanner;

/**
 * @author Gerry
 * @date 2022-08-05
 */
public class ManagementFactoryDemo {
    public static void main(String[] args) {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName();
        try {
            //获取目前该Java进程的方法
            System.out.println(Integer.parseInt(name.substring(0, name.indexOf('@'))));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
