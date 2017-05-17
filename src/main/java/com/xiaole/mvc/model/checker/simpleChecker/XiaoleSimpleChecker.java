package com.xiaole.mvc.model.checker.simpleChecker;

import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

/**
 * Created by llc on 17/3/24.
 */
public class XiaoleSimpleChecker implements CheckSimpleInterface {
    private static final Logger logger = Logger.getLogger(XiaoleSimpleChecker.class);

    @Override
    public boolean checkSimple(String slog) {
        return !getContentText(slog).equals("");
    }

    public String getContentText(String slog){
        String content;
        JSONObject jsonObject;
        try {
            JSONObject jlog = JSONObject.fromObject(slog);
            content = jlog.getString("content");
            jsonObject = JSONObject.fromObject(content);
            if (content.contains("nonFirstStart")){
                return "nonFirstStart  " + jsonObject.getString("sendModule");
            }
            else if (content.contains("sendContent")){
                return jsonObject.getString("sendContent")+" "+jsonObject.getString("sendType");
            }
            else if (content.contains("replyContent")){
                return jsonObject.getString("replyContent") + " " + jsonObject.getString("replyType");
            }
            else return "";
        } catch (Exception e) {
            logger.error("解析日志内容出错,错误日志:" + slog + ", 错误详情:" + e.getMessage());
            return  "";
        }
    }
}
