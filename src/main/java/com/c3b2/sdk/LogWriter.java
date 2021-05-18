package com.c3b2.sdk;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

public class LogWriter{
    private static final int BUFFER_LIMIT = 128 * 1024 * 1024;    // 128M

    private static LogWriter writer = null;
    private static String fileDate = null;
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
    private FileOutputStream outputStream = null;
    private String fileName = null;
    private StringBuilder msgBuffer = null;
    private static ObjectMapper objMapper = null;

    public static ObjectMapper buildObjectMapper() {
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objMapper.setPropertyNamingStrategy(
                PropertyNamingStrategies.SNAKE_CASE);
        objMapper.setTimeZone(TimeZone.getDefault());
        objMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        return objMapper;
    }

    public static synchronized LogWriter getInstance(String dataDir) throws Exception {
        if(writer == null){
            File path = new File(dataDir);
            if(!path.exists()){
                path.mkdirs();
            }
            objMapper = buildObjectMapper();
            String name = dataDir + "data_" + buildPid() + ".log";
            writer = new LogWriter(name);
        }
        return writer;
    }

    private static long buildPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        // eg: "pid@hostname"
        String name = runtime.getName();
        try {
            return Long.parseLong(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private LogWriter(String fileName) throws Exception {
        this.fileName = fileName;
        this.msgBuffer = new StringBuilder(8196);
        build();
    }

    private void build() throws Exception {
        fileDate = df.format(new Date());
        this.outputStream = new FileOutputStream(fileName + "." + fileDate, true);
    }

    public synchronized void append(Map<String, Object> dataMap) throws Exception {
        if (msgBuffer.length() < BUFFER_LIMIT) {
            try {
                msgBuffer.append(objMapper.writeValueAsString(dataMap));
                msgBuffer.append("\n");
            } catch (Exception ex) {
                throw new Exception("[fail to process json]", ex);
            }
        }else {
            throw new Exception("[buffer exceeded the allowed limitation]");
        }
        if (msgBuffer.length() >= 8196) {
            flush();
        }
    }

    public synchronized void flush() throws Exception {
        String fd = df.format(new Date());
        if (writer != null && !fd.equals(fileDate)) {
            writer.close();
        }
        if (outputStream == null) {
            try {
                build();
            } catch (Exception ex) {
                throw new Exception(ex);
            }
        }
        if (0 == msgBuffer.length()) {
            return;
        }
        if (writer.write(msgBuffer)) {
            msgBuffer.setLength(0);
        }
    }

    public synchronized boolean write(final StringBuilder sb) throws Exception {
        try {
            outputStream.write(sb.toString().getBytes("UTF-8"));
            outputStream.flush();
        } catch (Exception e) {
            throw new Exception("[write file fail]", e);
        }
        return true;
    }

    public void close() throws Exception {
        try {
            if(outputStream != null){
                outputStream.close();
            }
        } catch (Exception e) {
            throw new Exception("[close output stream fail]", e);
        }finally {
            outputStream = null;
        }
    }
}