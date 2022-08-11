package com.victor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.jsoup.Jsoup;

import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;


/**
 * @author Gerry
 * @date 2022-08-08
 */
@Slf4j
public class YarnClient {

    /**
     * yarn资源不能超过多少
     */
    private static final int YARN_RESOURCE = 55;

    /**
     * @return true : 表示资源正常， false: 资源紧张
     */
    public static boolean yarnResourceOk() {
        try {
            URL url = new URL("http://master:8088/cluster/scheduler");
            HttpURLConnection conn = null;
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setUseCaches(false);
            // 请求超时5秒
            conn.setConnectTimeout(5000);
            // 设置HTTP头:
            conn.setRequestProperty("Accept", "*/*");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36");
            // 连接并发送HTTP请求:
            conn.connect();

            // 判断HTTP响应是否200:
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("bad response");
            }
            // 获取所有响应Header:
            Map<String, List<String>> map = conn.getHeaderFields();
            for (String key : map.keySet()) {
                System.out.println(key + ": " + map.get(key));
            }
            // 获取响应内容:
            InputStream input = conn.getInputStream();
            byte[] datas = null;

            try {
                // 从输入流中读取数据
                datas = readInputStream(input);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String result = new String(datas, "UTF-8");// 将二进制流转为String

            Document document = Jsoup.parse(result);

            Elements elements = document.getElementsByClass("qstats");

            String[] ratios = elements.text().split("used");

            return Double.valueOf(ratios[3].replace("%", "")) < YARN_RESOURCE;
        } catch (IOException e) {
            log.error("yarn资源获取失败");
        }

        return false;

    }

    private static byte[] readInputStream(InputStream inStream) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = inStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, len);
        }
        byte[] data = outStream.toByteArray();
        outStream.close();
        inStream.close();
        return data;
    }
}
