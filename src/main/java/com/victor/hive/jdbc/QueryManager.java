package com.victor.hive.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class QueryManager {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");

    //使用堵塞队列实现生产者-消费者模式使用堵塞队列实现生产者-消费者模式
    public static final BlockingQueue<QueryBean> PENDING_QUEUE = new LinkedBlockingQueue<>();

    public static final Map<String, QueryBean> QUERY_MAP = new ConcurrentHashMap<>();

    private static AtomicInteger executorThreadNum = new AtomicInteger(0);
    private static int delay = 5;
    private boolean started = false;
    private HiveClient hiveClient = new HiveClient();

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
                                                               queryBean = hiveClient.executeQuery(queryBean);

                                                               //等待执行，获取执行日志和执行状态
                                                               queryBean = hiveClient.waitForOperationToComplete(queryBean);

                                                               //将计算结果放进Map中,等待前端获取,然后过期删除.目前简单处理仅是在内存中缓存,后续数据量大可以优化为放在redis等服务中
                                                               QUERY_MAP.put(queryId, queryBean);
                                                           } catch (Exception e) {
                                                               e.printStackTrace();
                                                               if (queryBean == null || StringUtils.isEmpty(queryId)) {
                                                                   return;
                                                               }

                                                               queryBean.queryState = QueryState.FAILED;
                                                               QUERY_MAP.put(queryId, queryBean);
                                                           }

//                                                           log.info("QUERY_BEAN:" + queryBean);
                                                           System.out.println("QUERY_BEAN:" + queryBean);
                                                       }
                                                   }
                                               },
                0,
                delay,
                TimeUnit.SECONDS);

        started = true;
    }





    /**
     * 线程池停止
     */
    public void stop() {
        if (started) {
            executorService.shutdown();
        }
        started = false;
    }






    /**
     * 将查询加入阻塞队列中
     *
     * @param queryBean
     */
    public static void addQueryBeanToPendingQueue(QueryBean queryBean) {
        queryBean.queryState = QueryState.WAITING;
        PENDING_QUEUE.offer(queryBean);
    }


}


