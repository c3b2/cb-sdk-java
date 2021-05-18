package com.c3b2.sdk;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class CbLog {

    private static LogWriter writer = null;
    private static ScheduledExecutorService flushExecutors = null;

    /**
     * init cb log
     * @param dataDir e.g: /cbdata/files/
     * @throws Exception
     */
    public static void init(String dataDir) throws Exception {
        writer = LogWriter.getInstance(dataDir);
        flushExecutors = Executors.newSingleThreadScheduledExecutor();
        flushExecutors.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    writer.flush();
                }catch (Exception ex){
                    //..
                }

            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * add dataMap
     * @param dataMap e.g:{event:'user_login',time:1620527456142,ip:'139.189.208.95',...}
     */
    public static void track(Map<String, Object> dataMap){
        try {
            writer.append(dataMap);
        }catch (Exception ex){
            //..
        }
    }

    /**
     * add dataMap
     * @param dataMap e.g:{event:'user_login',time:1620527456142,ip:'139.189.208.95',...}
     * @throws Exception
     */
    public static void trackEx(Map<String, Object> dataMap) throws Exception {
        writer.append(dataMap);
    }

    /**
     * close log
     */
    public static void close(){
        try {
            if(flushExecutors != null){
                flushExecutors.shutdownNow();
            }
        }catch (Exception ex1){
            //..
        }
        try {
            if(writer != null) {
                writer.flush();
                writer.close();
            }
        }catch (Exception ex2){
            //..
        }
    }
}
