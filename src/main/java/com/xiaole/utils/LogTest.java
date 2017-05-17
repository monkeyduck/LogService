package com.xiaole.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuxiao on 2017/3/24.
 */
public class LogTest {
    private static Logger logger = Logger.getLogger(LogTest.class);

    static String[]  inputStrings = new String[]{
            " {\"packageMeta\":{\"extraMap\":{\"secretKey\":\"0wQaZgvsv/P5DtVZxKUBunavEBC5oezxehqCr66MYwM\\u003d\",\"signature\":\"AD:D6:53:36:81:2B:3C:DC:29:22:AF:BE:9C:47:E4:74:EC:22:14:A7\",\"packageName\":\"com.aibasis.sdkdemo\"},\"clientType\":\"robot\",\"clientId\":\"GID_XIAOLE_A@@@0c:1d:af:df:1c:35\",\"deviceId\":\"0c:1d:af:df:1c:35\",\"protocolVersion\":\"v1.0\",\"packageUUID\":\"389ea562-890c-4a4d-8f11-65f91f9af50b\",\"priority\":1,\"createdTimestamp\":1490322823810,\"packageType\":\"common\",\"userId\":\"M0aeqao7Qd+zKnigGbKmzA\\u003d\\u003d\"},\"from\":{\"name\":\"controller\"},\"to\":{\"name\":\"dialog\"},\"packageContentList\":[{\"map\":{\"msg_content\":\"{\\\"ch_rec_list\\\":[{\\\"content\\\":\\\"你喜欢喝什么\\\"},{\\\"content\\\":\\\"你喜欢喝什么呢\\\"},{\\\"content\\\":\\\"你喜欢喝什么啊\\\"}]}\",\"location_info\":\"{\\\"longitude\\\":\\\"116.314791\\\",\\\"latitude\\\":\\\"39.975284\\\"}\",\"network_type\":\"wifi\",\"audio_url\":\"http://record-resource.oss-cn-beijing.aliyuncs.com/com.aibasis.sdkdemo/33469ea9-aa3b-41df-b32a-78a019b2a6cc/20170324/2017-03-24-10-33-43.wav\",\"msg_type\":\"text\"}}]}",
            "{\"packageMeta\":{\"extraMap\":{\"secretKey\":\"0wQaZgvsv/P5DtVZxKUBunavEBC5oezxehqCr66MYwM\\u003d\",\"reply_id\":\"389ea562\",\"signature\":\"AD:D6:53:36:81:2B:3C:DC:29:22:AF:BE:9C:47:E4:74:EC:22:14:A7\",\"sub_module_name\":\"protoss\",\"confidence\":\"0\",\"packageName\":\"com.aibasis.sdkdemo\"},\"clientType\":\"robot\",\"clientId\":\"GID_XIAOLE_A@@@0c:1d:af:df:1c:35\",\"deviceId\":\"0c:1d:af:df:1c:35\",\"protocolVersion\":\"v1.0\",\"packageUUID\":\"3211642f-6cf3-4df6-9c81-162f46dadabf\",\"priority\":1,\"createdTimestamp\":1490322824999,\"packageType\":\"common\",\"userId\":\"M0aeqao7Qd+zKnigGbKmzA\\u003d\\u003d\"},\"from\":{\"name\":\"dialog_ctr\"},\"to\":{\"name\":\"frontend\"},\"packageContentList\":[{\"map\":{\"msg_content\":\"{\\\"type\\\":\\\"clear\\\"}\",\"confidence\":\"1.1\",\"msg_type\":\"play_img\",\"reply_sub_module\":\"dialogtree\",\"action_after_execution\":\"no_listen\"}},{\"map\":{\"msg_content\":\"{\\\"content\\\":\\\"我觉得公交车司机真厉害，每次都拉着那么多乘客。\\\",\\\"face\\\":\\\"shining\\\"}\",\"confidence\":\"1.1\",\"msg_type\":\"play_tts\",\"reply_sub_module\":\"dialogtree\",\"action_after_execution\":\"no_listen\"}},{\"map\":{\"msg_content\":\"{\\\"content\\\":\\\"我们来玩当司机的游戏吧，现在你是司机，我是乘客哦，准备好了吗？\\\"}\",\"confidence\":\"1.1\",\"msg_type\":\"play_tts\",\"reply_sub_module\":\"dialogtree\"}}]}"
    };

    public static int catchTest() {
       try {
           int i = 10 / 0;   // 抛出 Exception，后续处理被拒绝
           System.out.println("i vaule is : " + i);
//           return 0;    // Exception 已经抛出，没有获得被执行的机会
       } catch (Exception e) {
            System.out.println(" -- Exception --");
//            return 1;    // Exception 抛出，获得了调用方法并返回方法值的机会
       }
        return 0;
    }

    public static void main(String[] args ){
        List<String> retList = new ArrayList<>();
        retList.add("abc\tedf\tadc");
        retList.add("123\t456\t567");
        List<String> r = new ArrayList<>();
        retList.forEach(l -> r.add(l.replace("\t", "kkkkk")));
        r.forEach(l -> System.out.println(l));
    }

    public static List<String> showLogs(String logLine){
        List<String> res = new ArrayList<>();
        try{
            JSONObject jobj = JSON.parseObject(logLine);
            if(jobj.containsKey("packageContentList")&&jobj.containsKey("packageMeta")){
                JSONObject pkgMeta = jobj.getJSONObject("packageMeta");
                String userId = pkgMeta.getString("userId");
                String from = jobj.getJSONObject("from").getString("name");
                String to = jobj.getJSONObject("to").getString("name");
                String audioUrl = "";
                JSONArray jarry = jobj.getJSONArray("packageContentList");

                for(int i =0;i<jarry.size();i++){
                    JSONObject OObj = jarry.getJSONObject(i);
                    JSONObject pcObj = OObj.getJSONObject("map");
                    if (pcObj.containsKey("msg_content")) {
                        String msgContentStr = pcObj.getString("msg_content");
                        String msgType = pcObj.getString("msg_type");
                        if(msgType.equals("text")){
                            audioUrl = pcObj.getString("audio_url");
                            JSONObject msgCont = JSON.parseObject(msgContentStr );
                            JSONArray ch_rec_list = msgCont.getJSONArray("ch_rec_list");
                            msgContentStr = ch_rec_list.getString(0);
                        }
                        Object action_after_execution = pcObj.get("action_after_execution");
                        if(action_after_execution==null){
                            action_after_execution = "default";
                        }
                        StringBuilder sb = new StringBuilder();
                        String line = from+"->"+to+"\t"+msgType+"\t"+msgContentStr+"\t"+ audioUrl + "\t" + action_after_execution;
                        res.add(line);
                    }

                }
            }else{
                //本条日志不合格
                res.add("本pc 格式不合格");
            }
        }catch (Exception e){
            logger.error(e.getMessage() + ", log: " + logLine);
        }
        return res;
    }

}
