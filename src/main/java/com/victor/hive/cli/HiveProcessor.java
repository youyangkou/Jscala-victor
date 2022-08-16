package com.victor.hive.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 该执行类必须与Hive节点部署在同一节点，且可直接执行hive -e命令
 *
 * @author Gerry
 * @date 2022-08-01
 */
public class HiveProcessor {

    public static void main(String[] args) throws IOException {

        String sql = "show tables; select * from db_real_sync_odps where tb='cdc_sync' and ds='20220720' limit 10";
        List<String> command = new ArrayList<String>();

        command.add("hive");
        command.add("-e");
        command.add(sql);

        List<String> results = new ArrayList<String>();
        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);
        Process hiveProcess = hiveProcessBuilder.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                hiveProcess.getInputStream()));

        String data = null;
        while ((data = br.readLine()) != null) {
            results.add(data);
        }

        System.out.println("开始打印hive执行结果");
        for (String result : results) {
            System.out.println(result);
        }

    }

}
