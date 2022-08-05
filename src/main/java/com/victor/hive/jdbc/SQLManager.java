package com.victor.hive.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SQLManager {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");
    //使用堵塞队列实现生产者-消费者模式
    public static final BlockingQueue<SQLBean> PENDING_QUEUE = new LinkedBlockingQueue<>();
    public static final Map<String, SQLResult> RESULT_MAP = new ConcurrentHashMap<>();
    private static AtomicInteger executorThreadNum = new AtomicInteger(0);

    //    private static int executeDuration = SystemCon.getInt(SystemConfig.HIVE_SQL_EXECUTE_DURATION);
    private static int executeDuration = 60000;
    private boolean started = false;
    ScheduledExecutorService executorService;


    public void start() {
        if (started) {
            return;
        }

        System.out.println("开始创建线程池");
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
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
                                                           log.info("PENDING_QUEUE size:" + PENDING_QUEUE.size());

                                                           System.out.println("PENDING_QUEUE size:" + PENDING_QUEUE.size());

                                                           SQLBean sqlBean = null;
                                                           String queryId = null;
                                                           try {
                                                               //从堵塞队列中取出一个计算任务
                                                               sqlBean = PENDING_QUEUE.take();
                                                               queryId = sqlBean.getQueryId();
                                                               String result = executeSQL(sqlBean);

                                                               SQLResult sqlResult = new SQLResult(queryId, result,
                                                                       SQLResult.SUCCESS_STATUS,
                                                                       SQLResult.SUCCESS_MSG);
                                                               //将计算结果放进Map中,等待前端获取,然后过期删除.目前简单处理仅是在内存中缓存,后续数据量大可以优化为放在redis等服务中
                                                               RESULT_MAP.put(queryId, sqlResult);

                                                           } catch (Exception e) {
                                                               e.printStackTrace();
                                                               if (sqlBean == null || StringUtils.isEmpty(queryId)) {
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
                executeDuration,
                TimeUnit.SECONDS);

        started = true;

    }

    public void stop() {
        if (started) {
            executorService.shutdown();
        }
        started = false;
    }


    public static void addSqlBeanToPendingQueue(SQLBean sqlBean) {
        PENDING_QUEUE.offer(sqlBean);
    }


    public String executeSQL(SQLBean sqlBean) throws SQLException, ClassNotFoundException, ExecutionException {
        //若是提供queryId的即是计算请求(如count计算),没有的则是非计算请求(如DDL语句),非计算请求使用缓存内的连接执行.
        /*HiveClient hiveClient = Strings.isNullOrEmpty(sqlBean.getQueryId()) ? Cache.getHiveClientCache().get("common-client")
                : HiveClientFactory.getSinglComputeClient();*/
        HiveClient hiveClient =new HiveClient();

        String result = hiveClient.executeQuery(sqlBean);
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


