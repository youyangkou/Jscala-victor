package com.victor.hive.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class QueryManager {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");

    //使用堵塞队列实现生产者-消费者模式使用堵塞队列实现生产者-消费者模式
    public static final BlockingQueue<QueryBean> PENDING_QUEUE = new LinkedBlockingQueue<>();

    public static final Map<String, QueryBean> QUERY_MAP = new ConcurrentHashMap<>();
    public static final Map<String, SQLResult> RESULT_MAP = new ConcurrentHashMap<>();

    private static AtomicInteger executorThreadNum = new AtomicInteger(0);
    private static int delay = 5;
    private boolean started = false;
    private HiveClient hiveClient =new HiveClient();

    ScheduledExecutorService executorService;


    public void start() {
        if (started) {
            return;
        }

        System.out.println("开始创建线程池");
        executorService = Executors.newScheduledThreadPool(5, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SQLManager-Thread-" + executorThreadNum.getAndAdd(1));
                return t;
            }
        });

        System.out.println("开始定时执行SQL");
        //定时执行sql计算任务
        executorService.scheduleWithFixedDelay(new Runnable() {
                                                   @Override
                                                   public void run() {
                                                       while (PENDING_QUEUE.size() > 0) {
//                                                           log.info("PENDING_QUEUE size:" + PENDING_QUEUE.size());
                                                           System.out.println("PENDING_QUEUE size:" + PENDING_QUEUE.size());

                                                           QueryBean queryBean = null;
                                                           String queryId = null;
                                                           try {
                                                               //从堵塞队列中取出一个计算任务
                                                               queryBean = PENDING_QUEUE.take();
                                                               queryId = queryBean.getQueryId();
                                                               //异步提交
                                                               String result = executeSQL(queryBean);
//                                                               hiveClient.waitForOperationToComplete(hiveClient.statementMap.get(queryBean),queryBean);



                                                               SQLResult sqlResult = new SQLResult(queryId, result,
                                                                       SQLResult.SUCCESS_STATUS,
                                                                       SQLResult.SUCCESS_MSG);
                                                               //将计算结果放进Map中,等待前端获取,然后过期删除.目前简单处理仅是在内存中缓存,后续数据量大可以优化为放在redis等服务中
                                                               RESULT_MAP.put(queryId, sqlResult);

                                                           } catch (Exception e) {
                                                               e.printStackTrace();
                                                               if (queryBean == null || StringUtils.isEmpty(queryId)) {
                                                                   return;
                                                               }

                                                               SQLResult sqlResult = new SQLResult(queryId, "",
                                                                       SQLResult.FAILED_STATUS,
                                                                       e.getMessage());
                                                               RESULT_MAP.put(queryId, sqlResult);
                                                           }

                                                           log.info("RESULT_MAP:" + RESULT_MAP);

                                                           System.out.println("RESULT_MAP:" + RESULT_MAP);
                                                       }
                                                   }
                                               },
                0,
                delay,
                TimeUnit.SECONDS);

        started = true;
    }

    public void stop() {
        if (started) {
            executorService.shutdown();
        }
        started = false;
    }


    public static void addQueryBeanToPendingQueue(QueryBean queryBean) {
        PENDING_QUEUE.offer(queryBean);
    }





    public String executeSQL(QueryBean queryBean) throws SQLException, ClassNotFoundException, ExecutionException {
        String result = hiveClient.executeQuery(queryBean);
        if (result == null) return "";
        return result;
    }









    //存放hive计算结果的内部类
    public class SQLResult {

        public static final String SUCCESS_STATUS = "success";
        public static final String SUCCESS_MSG = "ok";
        public static final String FAILED_STATUS = "failed";

        String queryId;
        String result;
        String status;
        String message;

        //记录结果产生时间,用于定时删除
        String generateTime = TIME_FORMATTER.print(System.currentTimeMillis());
        boolean checked = false;

        public SQLResult() {
        }

        public SQLResult(String queryId, String result, String status, String message) {
            this.queryId = queryId;
            this.result = result;
            this.status = status;
            this.message = message;
        }

        public String getQueryId() {
            return queryId;
        }

        public String getResult() {
            return result;
        }

        public String getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getGenerateTime() {
            return generateTime;
        }

        public SQLResult setChecked(boolean checked) {
            this.checked = checked;
            return this;
        }

        @Override
        public String toString() {
            return "SQLResult{" +
                    "queryId='" + queryId + '\'' +
                    ", result='" + result + '\'' +
                    ", status='" + status + '\'' +
                    ", message='" + message + '\'' +
                    ", generateTime='" + generateTime + '\'' +
                    ", checked=" + checked +
                    '}';
        }
    }

}


