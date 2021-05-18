package sdk;

import com.c3b2.sdk.CbLog;

import java.util.HashMap;
import java.util.Map;

public class Demo {

    public static void main(String[] agrs){
        try {
            CbLog.init("/cbdata/files/");

            Map<String,Object> dataMap = new HashMap<>();
            dataMap.put("event","user_login");
            dataMap.put("time", System.currentTimeMillis());
            dataMap.put("os","android");
            dataMap.put("channel_id","qq");
            for(int iter = 0; iter < 10; iter++){
                dataMap.put("device_id", "d_" + iter);

                CbLog.track(dataMap);
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            CbLog.close();
        }
    }
}
