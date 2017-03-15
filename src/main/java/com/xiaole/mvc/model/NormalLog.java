package com.xiaole.mvc.model;

import com.xiaole.mvc.model.checker.SdkLogChecker;
import net.sf.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public String toNewSimpleFormat() {
        if (SdkLogChecker.check(this)) {
            String ret = "";
            if (jsonContent.containsKey("from") && jsonContent.containsKey("to")) {
                ret = time + " " + level + " " + jsonContent.getJSONObject("from").getString("name") + "->"
                        + jsonContent.getJSONObject("to").getString("name") + " " + parseContent() + " "
                        + member_id;
            }
            return ret;
        } else {
            return toReadableSimpleLog();
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
        String str = "{\n" +
                "    \"packageMeta\": {\n" +
                "        \"extraMap\": {\n" +
                "            \"secretKey\": \"0wQaZgvsv/P5DtVZxKUBunavEBC5oezxehqCr66MYwM=\",\n" +
                "            \"reply_id\": \"19fc3ad9\",\n" +
                "            \"signature\": \"AD:D6:53:36:81:2B:3C:DC:29:22:AF:BE:9C:47:E4:74:EC:22:14:A7\",\n" +
                "            \"packageName\": \"com.aibasis.sdkdemo\"\n" +
                "        },\n" +
                "        \"clientType\": \"robot\",\n" +
                "        \"clientId\": \"GID_XIAOLE_A@@@0c:1d:af:df:1c:35\",\n" +
                "        \"deviceId\": \"0c:1d:af:df:1c:35\",\n" +
                "        \"protocolVersion\": \"v1.0\",\n" +
                "        \"packageUUID\": \"92973b4a-dd38-48b0-b842-3d6c08b55bf0\",\n" +
                "        \"priority\": 1,\n" +
                "        \"createdTimestamp\": 1489544866618,\n" +
                "        \"packageType\": \"common\",\n" +
                "        \"userId\": \"M0aeqao7Qd+zKnigGbKmzA==\"\n" +
                "    },\n" +
                "    \"from\": {\n" +
                "        \"name\": \"dialog_ctr\"\n" +
                "    },\n" +
                "    \"to\": {\n" +
                "        \"name\": \"frontend\"\n" +
                "    },\n" +
                "    \"packageContentList\": [\n" +
                "        {\n" +
                "            \"map\": {\n" +
                "                \"msg_content\": {\n" +
                "                    \"content\": \"我会画飞机，你会画飞机吗？\"\n" +
                "                },\n" +
                "                \"msg_type\": \"play_tts\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        JSONObject json = JSONObject.fromObject(str);
        JSONObject contentMap = json.getJSONArray("packageContentList").getJSONObject(0);
        System.out.println(contentMap.getJSONObject("map").getJSONObject("msg_content").getJSONArray("ch_rec_list").getJSONObject(0).getString("content"));
        String url = "all";
        String newUrl = "";
        try {
            String encode = java.net.URLEncoder.encode(url, "UTF-8");
            System.out.println("encode: " + encode);
            newUrl = java.net.URLDecoder.decode(encode, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(newUrl);
    }
}
