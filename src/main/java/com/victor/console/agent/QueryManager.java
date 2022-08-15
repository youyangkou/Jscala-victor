package com.victor.console.agent;

import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import com.victor.utils.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class QueryManager {

    public static final BlockingQueue<QueryInstance> PENDING_QUEUE = new LinkedBlockingQueue<>();
    public static final Map<String, QueryInstance> QUERY_MAP = new ConcurrentHashMap<>();

    private static AtomicInteger executorThreadNum = new AtomicInteger(0);
    private static int delay = 5;
    private boolean started = false;
    private HiveClient hiveClient = new HiveClient();
    ScheduledExecutorService executorService;

    @Autowired
    HiveQueryService hiveQueryService;

    public QueryManager() {
    }


    public void start() {
        if (started) {
            return;
        }

        System.out.println("开始创建线程池");
        log.info("开始创建线程池");
        executorService = Executors.newScheduledThreadPool(5, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("SQLManager-Thread-" + executorThreadNum.getAndAdd(1));
                return thread;
            }
        });

        System.out.println("开始定时执行SQL");
        log.info("开始定时执行SQL");
        //定时执行sql计算任务
        executorService.scheduleWithFixedDelay(new Runnable() {
                                                   @Override
                                                   public void run() {
                                                       while (PENDING_QUEUE.size() > 0) {
                                                           log.info("PENDING_QUEUE size:" + PENDING_QUEUE.size());
                                                           System.out.println("PENDING_QUEUE size:" + PENDING_QUEUE.size());

                                                           HiveQueryBean hiveQueryBean;
                                                           QueryInstance queryInstance = null;
                                                           String queryId = null;
                                                           try {
                                                               //从堵塞队列中取出一个计算任务
                                                               queryInstance = PENDING_QUEUE.take();
                                                               queryId = queryInstance.getQueryId();
                                                               //异步提交
                                                               queryInstance = hiveClient.executeQuery(queryInstance);

                                                               if (!queryInstance.isOnlyQuery) {
                                                                   //等待执行，获取执行日志和执行状态
                                                                   queryInstance = hiveClient.waitForOperationToComplete(queryInstance);
                                                                   //更新状态
                                                                   hiveQueryBean = hiveQueryService.get(queryInstance.queryId);
                                                                   hiveQueryBean.setQueryState(queryInstance.queryState.getQueryState());
                                                                   hiveQueryBean.setLog(queryInstance.log);
                                                                   hiveQueryService.update(hiveQueryBean);
                                                               }

                                                               //将计算结果放进Map中,等待前端获取,然后过期删除.目前简单处理仅是在内存中缓存,后续数据量大可以优化为放在redis等服务中
                                                               QUERY_MAP.put(queryId, queryInstance);
                                                           } catch (Exception e) {
                                                               e.printStackTrace();
                                                               if (queryInstance == null || StringUtils.isEmpty(queryId)) {
                                                                   return;
                                                               }

                                                               queryInstance.queryState = QueryState.FAILED;
                                                               //更新状态
                                                               hiveQueryBean = hiveQueryService.get(queryInstance.queryId);
                                                               hiveQueryBean.setQueryState(queryInstance.queryState.getQueryState());
                                                               hiveQueryBean.setLog(queryInstance.log);
                                                               hiveQueryService.update(hiveQueryBean);
                                                               QUERY_MAP.put(queryId, queryInstance);
                                                           }

                                                           log.info("QUERY_BEAN:" + queryInstance);
                                                           System.out.println("QUERY_BEAN:" + queryInstance);
                                                       }
//                                                       System.out.println(QUERY_MAP);
                                                       System.out.println("队列执行结束");
                                                       System.out.println("===========================================================================================");
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
     * @param queryInstance
     */
    public static void addQueryBeanToPendingQueue(QueryInstance queryInstance) {
        queryInstance.queryState = QueryState.WAITING;
        PENDING_QUEUE.offer(queryInstance);
    }


    /**
     * 取消查询
     *
     * @param queryInstance
     * @return
     * @throws SQLException
     */
    public boolean cancelQuery(QueryInstance queryInstance) throws SQLException {
        boolean result = hiveClient.cancelQuery(queryInstance);
        if (result) {
            //更新状态
            HiveQueryBean hiveQueryBean = hiveQueryService.get(queryInstance.queryId);
            hiveQueryBean.setQueryState(queryInstance.queryState.getQueryState());
            hiveQueryBean.setLog(queryInstance.log);
            hiveQueryService.update(hiveQueryBean);
        }
        return result;
    }


    /**
     * 初始化查询对象
     *
     * @param project
     * @param sql
     * @param isOnlyQuery
     * @return
     */
    public static QueryInstance generateQueryBean(String project, String sql, boolean isOnlyQuery) {

        if (org.apache.commons.lang3.StringUtils.isEmpty(project)) {
            project = "default";
        }
        if (org.apache.commons.lang3.StringUtils.isEmpty(sql)) {
            throw new IllegalArgumentException("sql must not be empty");
        }
        if (!sql.toLowerCase().trim().startsWith("select")) {
            throw new IllegalArgumentException(
                    "Prohibit submission of queries that do not start with select");
        }
        if (sql.trim().endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        String queryId = String.valueOf(sql.toLowerCase().replaceAll(" ", "").trim().hashCode());
        String tmpTable = "";

        if (!isOnlyQuery) {
            String ds = TimeUtil.getToday();
            tmpTable = "tmp_" + UUID.randomUUID().toString().replace("-", "_") + "_" + ds;

            StringBuilder ddlSQL = new StringBuilder("Create Table ")
                    .append(project)
                    .append(".")
                    .append(tmpTable)
//                    .append(" lifecycle 1 as ")
                    .append(" as ")
                    .append(sql);

            sql = ddlSQL.toString();
        }

        return QueryInstance.builder()
                .project(project)
                .querySql(sql)
                .tmpTable(tmpTable)
                .queryId(queryId)
                .isOnlyQuery(isOnlyQuery)
                .build();
    }


}


