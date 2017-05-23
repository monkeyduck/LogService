package com.xiaole.mvc.model;

import com.xiaole.mvc.model.checker.SdkLogChecker;
import com.xiaole.utils.LogTest;
import net.sf.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * 日志服务
 * Created by hxx on 3/15/16.
 */
public class NormalLog {

    private static final Logger logger = LoggerFactory.getLogger(NormalLog.class);
    private String content;
    private String member_id;
    private String device_id;
    private String modtrans;
    private String ip;
    private EnvironmentType envType;
    private String time;
    private String log_time;
    private String level;
    private JSONObject jsonContent;


    public NormalLog(String log) throws Exception {
        JSONObject json = JSONObject.fromObject(log);
        level = json.getString("level");
        member_id = replaceNull(json.getString("memberId"));
        modtrans = replaceNull(json.getString("module"));
        ip = replaceNull(json.getString("ip"));
        device_id = replaceNull(json.getString("deviceId"));
        String environment = replaceNull(json.getString("environment"));
        log_time = replaceNull(json.getString("timeStamp"));
        DateTime dt = new DateTime(Long.parseLong(log_time));
        time = dt.toString("yyyy-MM-dd HH:mm:ss.SSS");
        envType = EnvironmentType.fromString(environment);
        content = json.getString("content");
        jsonContent = JSONObject.fromObject(content);
    }

    public String getTime(){
        return time;
    }

    public void setTime(String time){
        this.time = time;
    }
    public static Logger getLogger() {
        return logger;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getMember_id() {
        return member_id;
    }

    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getModtrans() {
        return modtrans;
    }

    public void setModtrans(String modtrans) {
        this.modtrans = modtrans;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public EnvironmentType getEnvType() {
        return envType;
    }

    public void setEnvType(EnvironmentType envType) {
        this.envType = envType;
    }

    public String getLog_time() {
        return log_time;
    }

    public void setLog_time(String log_time) {
        this.log_time = log_time;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    private static String replaceNull(String st) {
        return st == null ? "" : st;
    }

    public boolean containUsedTime(){
        if (containMethodName()){
            JSONObject json = JSONObject.fromObject(content);
            String methodName = json.getString("methodName");
            return methodName.equals("usedTime") && !modtrans.equals("FrontEnd");
        }
        return false;
    }

    public boolean containMethodName(){
        return content.contains("methodName") && !modtrans.equals("preprocess") && !member_id.equals("init");
    }

    public String getMethodName(){
        String methodName = "";
        if (jsonContent != null && jsonContent.containsKey("methodName"))
            methodName = jsonContent.getString("methodName");
        return methodName;
    }

    public String getUsedTime(){
        JSONObject json = JSONObject.fromObject(content);
        long fromTime = -1;
        long toTime = -1;
        if (containUsedTime()){
            String sfromTime = json.getString("fromTime");
            String stoTime = json.getString("toTime");
            if (!sfromTime.equals("") && !stoTime.equals("")) {
                try{
                    fromTime = Long.parseLong(sfromTime);
                    toTime = Long.parseLong(stoTime);
                    if (fromTime > 0 && toTime > 0 && toTime > fromTime) {
                        long usePeriods = toTime - fromTime;
                        return "" + usePeriods;
                    }
                }catch (Exception e){
                    logger.error(e.getMessage());
                }

            }
        }
        return "0";
    }

    static private String processContent(String memberId, String timeStamp, String module, String content)
            throws Exception{
        if (module.equals("") || memberId.equals("") || timeStamp.equals("")) {
            String message = "module, memberId and timeStamp should have value";
            throw new Exception(message);
        }
        return content;
    }

    public boolean belongsToSimple(){
        if (!this.getContentText().equals(""))
            return true;
        else
            return false;
    }

    public String getContentText(){
        if (jsonContent != null) {
            if (content.contains("nonFirstStart") && jsonContent.containsKey("sendModule")){
                return "nonFirstStart  "+jsonContent.getString("sendModule");
            }
            else if (jsonContent.containsKey("sendContent")) {
                return jsonContent.getString("sendContent")+" "+jsonContent.getString("sendType");
            }
            else if (jsonContent.containsKey("replyContent")) {
                return jsonContent.getString("replyContent") + " " + jsonContent.getString("replyType");
            }
            else return "";
        }
        else return "";
    }

    public String getShortMem(){
        if (this.member_id.contains(".")) {
            String[] segs = member_id.split("\\.");
            return segs[1];
        }
        else
            return member_id;
    }

    public String getVersion(){
        if (jsonContent.containsKey("version")){
            return jsonContent.getString("version");
        }
        else if (jsonContent.containsKey("softwareVersion")){
            return jsonContent.getString("softwareVersion");
        }
        else{
            return "";
        }
    }

    public String getAudioRecordID(){
        if (jsonContent.containsKey("audioRecordId"))
            return jsonContent.getString("audioRecordId");
        else
            return "";
    }

    public String getRecordLink(){
        String record_id = getAudioRecordID();
        if (!record_id.equals("")){
            return "<a href=\"http://record-resource.oss-cn-beijing.aliyuncs.com/"+this.member_id+"/"+record_id+
                    "\">录音链接</a>";
        }
        else
            return "";

    }

    public String toSimpleLog(){
        String re;
        if (!getRecordLink().equals("")){
            re = time+"&nbsp;&nbsp;&nbsp;&nbsp;"+level+"&nbsp;&nbsp;&nbsp;&nbsp;"+modtrans.replaceAll(">","&gt;")
                    +"&nbsp;&nbsp;&nbsp;&nbsp;"+getContentText()+"&nbsp;&nbsp;&nbsp;&nbsp;"+getRecordLink()
                    +"&nbsp;&nbsp;&nbsp;&nbsp;"+getShortMem()+"&nbsp;&nbsp;&nbsp;&nbsp;"+getVersion();
        }
        else{
            re=time+"&nbsp;&nbsp;&nbsp;&nbsp;"+level+"&nbsp;&nbsp;&nbsp;&nbsp;"+modtrans.replaceAll(">","&gt;")
                    +"&nbsp;&nbsp;&nbsp;&nbsp;"+getContentText()+"&nbsp;&nbsp;&nbsp;&nbsp;"+getShortMem()
                    +"&nbsp;&nbsp;&nbsp;&nbsp;"
                    +getVersion();
        }
        return re;
    }

    public String toReadableSimpleLog(){
        String re;
        if (!getRecordLink().equals("")){
            re = time+"\t"+level+"\t"+modtrans+"\t"+getContentText()+"\t"+getRecordLink()
                    +"\t"+getShortMem()+"\t"+getVersion();
        }
        else{
            re=time+"\t"+level+"\t"+modtrans+"\t"+getContentText()+"\t"+getShortMem()
                    +"\t"+getVersion();
        }
        return re;
    }

    public boolean isTrans() {
        return modtrans.contains("->");
    }

    public List<String> toNewSimpleFormat() {
        if (SdkLogChecker.check(this)) {
            List<String> ret = new ArrayList<>();
            List<String> contents = LogTest.showLogs(content);
            for (String con : contents) {
                ret.add(time + " " + level + " " + con + " " + member_id);
            }
            return ret;
        } else {
            List<String> xiaoleList = new ArrayList<>();
            if (toReadableSimpleLog() != null) {
                xiaoleList.add(toReadableSimpleLog());
            }
            return xiaoleList;
        }
    }

    public String parseContent() {
        try {
            JSONObject contentMap = jsonContent.getJSONArray("packageContentList").getJSONObject(0).getJSONObject("map");
            String type = contentMap.getString("msg_type");
            if (type.equals("text")) {
                return contentMap.getJSONObject("msg_content").getJSONArray("ch_rec_list").getJSONObject(0).getString("content");
            } else {
                return contentMap.getJSONObject("msg_content").getString("content");
            }
        } catch (Exception e) {
            logger.error("error in parseContent, logContent: " + jsonContent.toString());
            return "";
        }
    }

    public static void main(String[] args) {
        String log = "{\"timeStamp\":1495193042513,\"environment\":\"alpha\",\"level\":\"INFO\",\"module\":\"cockroach\",\"ip\":\"127.0.0.1\",\"from\":\"instant_chat\",\"to\":\"frontend\",\"deviceId\":\"18:97:ff:04:66:b7\",\"content\":\"{\\\"packageMeta\\\":{\\\"extraMap\\\":{},\\\"clientType\\\":\\\"robot\\\",\\\"clientId\\\":\\\"GID_TOPIC_XIAOLE_UP_A@@@1698896608\\\",\\\"deviceId\\\":\\\"18:97:ff:04:66:b7\\\",\\\"protocolVersion\\\":\\\"v1.0\\\",\\\"packageUUID\\\":\\\"28b1ad30-1243-429a-92ac-52cb89ea8768\\\",\\\"priority\\\":1,\\\"createdTimestamp\\\":1495193042466,\\\"packageType\\\":\\\"common\\\",\\\"userId\\\":\\\"1698896608\\\"},\\\"from\\\":{\\\"name\\\":\\\"instant_chat\\\"},\\\"to\\\":{\\\"name\\\":\\\"frontend\\\"},\\\"packageContentList\\\":[{\\\"map\\\":{\\\"message_time\\\":\\\"1495193042466\\\",\\\"command_type\\\":\\\"home_id_speech\\\",\\\"content\\\":\\\"我们住在快乐星球6 6 0 8号\\\",\\\"call_id\\\":\\\"1698896608\\\"}}]}\",\"memberId\":\"1698896608\",\"@version\":\"1\",\"@timestamp\":\"2017-05-19T19:24:02.513+08:00\",\"path\":\"/home/wangzhiwei/cockroach/stats_logs/2017-05-19.log\",\"host\":\"iZ2ze08boib6nvrf09g9g7Z\",\"type\":\"stats\",\"debug\":\"timestampMatched\",\"hdfs_filename\":\"2017-05-19\",\"redis_key\":\"2017-05-19.19:24\"}\n";
        try {
            NormalLog normalLog = new NormalLog(log);
            System.out.println(normalLog.toNewSimpleFormat());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
