package com.xiaole.mvc.controller;

import com.xiaole.elasticsearch.ELServer;
import com.xiaole.hdfs.HDFSManager;
import com.xiaole.redis.IKVStore;
import com.xiaole.utils.DownloadFileUtil;
import com.xiaole.utils.Utils;
import net.sf.json.JSONArray;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by llc on 16/11/17.
 */

@RestController
@RequestMapping("/log")
public class Controller {

    private static final Logger logger = Logger.getLogger(Controller.class);

    @Autowired
    private IKVStore redis;

    @Autowired
    private HDFSManager hdfsManager;

    @Autowired
    private ELServer elServer;

    @RequestMapping("/search/latest")
    public JSONArray getLatestLog(@RequestParam("minute") int minute) {
        List<String> ret = new ArrayList<String>();
        DateTime dateTime = new DateTime();
        for (int i = minute; i >= 0; --i) {
            String time = dateTime.minusMinutes(i).toString("yyyy-MM-dd.HH:mm");
            String key = "stats-" + time;
            logger.info("redis key: " + key);
            ret.addAll(redis.strLget(key));
        }
        return JSONArray.fromObject(ret);
    }


    @RequestMapping("/search/between-timestamp")
    public String getLogBetweenTimeStampEl(@RequestParam("startTimeStamp") String startTimeStamp,
                                            @RequestParam("endTimeStamp") String endTimeStamp,
                                            @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                            @RequestParam(value = "module", defaultValue = "all") String module,
                                            @RequestParam(value = "level", defaultValue = "all") String level,
                                            @RequestParam(value = "env", defaultValue = "all") String env) {
        long sts = 0;
        long ets = 0;
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return "memberId 解析错误!";
        }
        try {
            sts = Long.parseLong(startTimeStamp);
            ets = Long.parseLong(endTimeStamp);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "时间戳格式错误!";
        }
        long maxRange = 24 * 60 * 60 * 1000;
        if (ets < sts) {
            return "截止时间戳小于起始时间戳";
        } else if (ets - sts > maxRange) {
            return "时间戳范围超过24小时, 请使用其它接口查询";
        } else
            return elServer.getLogBetweenTimeStamp(sts, ets, memberId, module, level, env);
    }


    @RequestMapping("/search/between-date")
    public String getLogBetweenDate(@RequestParam("startDate") String sdate,
                                  @RequestParam("endDate") String edate,
                                  @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                  @RequestParam(value = "module", defaultValue = "all") String module,
                                  @RequestParam(value = "level", defaultValue = "all") String level,
                                    @RequestParam(value = "env", defaultValue = "all") String env) {
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return "memberId 解析错误!";
        }
        DateTime sdt = new DateTime(sdate);
        DateTime edt = new DateTime(edate);
        int range = Days.daysBetween(sdt, edt).getDays();
        if (range < 0) {
            return "参数输入错误: 起始日期应小于截止日期";
        } else if (range == 0) {
            return elServer.getLogByDateFromELSearch(sdate, memberId, module, level, env);
        } else {
            return elServer.getLogBetweenDate(sdate, edate, range, memberId, module, level, env);
        }
    }

    @RequestMapping("/search/realtime")
    public JSONArray getRealTimeLogByDate(@RequestParam("date") String date,
                               @RequestParam(value = "memberId") String memberId,
                               @RequestParam(value = "level", defaultValue = "all") String level,
                               @RequestParam(value = "env", defaultValue = "all") String env,
                               @RequestParam(value = "source", defaultValue = "xiaole") String src) {
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return null;
        }
        List<String> retList = elServer.getSimpleLogByDate(date, memberId, env, level, src);
        List<String> replaceList = new ArrayList<>();
        retList.forEach(l -> replaceList.add(l.replace("\t", " ")));
        if (retList != null) {
            return JSONArray.fromObject(replaceList);
        } else {
            return null;
        }
    }

    @RequestMapping("/search/date")
    public String getLogByDate(@RequestParam("date") String date,
                               @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                               @RequestParam(value = "module", defaultValue = "all") String module,
                               @RequestParam(value = "level", defaultValue = "all") String level,
                               @RequestParam(value = "env", defaultValue = "all") String env) {
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return "memberId 解析错误!";
        }
        return elServer.getLogByDateFromELSearch(date, memberId, module, level, env);
    }

    @RequestMapping("/search/latestChatLog")
    public JSONArray getLatestChatLog(@RequestParam("count") int count,
                                      @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                      @RequestParam(value = "level", defaultValue = "all") String level,
                                      @RequestParam(value = "env", defaultValue = "all") String env) {
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return null;
        }
        DateTime dt = new DateTime();
        String date = dt.toString("yyyy-MM-dd");
        JSONArray jsonArray = elServer.getChatLogByNumber(count, date, memberId, level, env);
        return jsonArray;
    }

    @RequestMapping("/search/recentChatLog")
    public JSONArray getRecentChatLog(@RequestParam("startTimeStamp") long startTimeStamp,
                                      @RequestParam("endTimeStamp") long endTimeStamp,
                                      @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                      @RequestParam(value = "level", defaultValue = "all") String level,
                                      @RequestParam(value = "env", defaultValue = "all") String env) {
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return null;
        }
        JSONArray jsonArray = elServer.getChatLogByTimeStamp(startTimeStamp, endTimeStamp, memberId, level, env);
        return jsonArray;
    }

    @RequestMapping("/download/date")
    public void downloadLogByDate(HttpServletResponse response,
                                  @RequestParam("date") String date,
                                  @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                  @RequestParam(value = "module", defaultValue = "all") String module,
                                  @RequestParam(value = "level", defaultValue = "all") String level,
                                  @RequestParam(value = "env", defaultValue = "all") String env,
                                  @RequestParam(value = "source", defaultValue = "xiaole") String src){
        logger.info("Start to download log of date: " + date);
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return ;
        }
        try{
            String shortId = memberId;
            if (memberId.contains(".")) {
                shortId = memberId.split("\\.")[1];
            }
            String saveName = src + "_" + date + "_" + module + "_" + shortId + "_" + level + ".txt";

            DateTime today = new DateTime();
            DateTime searchDay = new DateTime(date);
            List<String> logList;
            // 十五天以内的日志从elasticsearch中获取下载
            if (today.getDayOfYear() - searchDay.getDayOfYear() < 15) {
                logList = elServer.getComplexLogByDate(memberId, date, module, level, env, src);
                DownloadFileUtil.downloadLogByList(logList, saveName, response);
            } else { //十五天以上的从hdfs中下载
                if (memberId.equals("all") && module.equals("all") && env.equals("all") && level.equals("all")) {
                    hdfsManager.downloadLog(date, saveName, response);
                } else {
                    logList = hdfsManager.getLogByDate(date, module, memberId, env, level, src);
                    DownloadFileUtil.downloadLogByList(logList, saveName, response);
                }
            }

        } catch (IOException e){
            logger.error("error when downloading log: " + date + "! Caused by: " + e.getMessage());
        }
    }

    @RequestMapping("/download-simple/date")
    public void downloadSimpleLogByDate(HttpServletResponse response,
                                        @RequestParam("date") String date,
                                        @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                        @RequestParam(value = "module", defaultValue = "all") String module,
                                        @RequestParam(value = "env", defaultValue = "all") String env,
                                        @RequestParam(value = "level", defaultValue = "all") String level,
                                        @RequestParam(value = "source", defaultValue = "xiaole") String src){
        logger.info("Start to download simple log of date: " + date);
        memberId = Utils.decodeUrl(memberId);
        if (memberId.equals("")) {
            return ;
        }
        try{
            DateTime today = new DateTime();
            DateTime searchDay = new DateTime(date);

            String shortId = memberId;
            if (memberId.contains(".")) {
                shortId = memberId.split("\\.")[1];
            }
            String saveName = src + "_simple_" + date + "_" + module + "_" + shortId + "_" + level + ".txt";
            // 十五天以内的日志从elasticsearch中获取下载
            if (today.getDayOfYear() - searchDay.getDayOfYear() < 15) {
                if (memberId.equals("all")) {
                    logger.info("Download all members logs");
                    Map<String, List<String>> logMap = elServer.getAllUserSimpleLog(date, env, level, src);
                    DownloadFileUtil.downloadLogByMap(logMap, saveName, response);
                } else {
                    logger.info("Download logs of member: " + memberId);
                    List<String> logList = elServer.getSimpleLogByDate(date, memberId, env, level, src);
                    DownloadFileUtil.downloadLogByList(logList, saveName, response);
                }
            } else { // 十五天及以上的从hdfs中获取下载
                if (memberId.equals("all")) {
                    Map<String, List<String>> logMap = hdfsManager.getAllUserSimpleLog(date, module, env, level, src);
                    DownloadFileUtil.downloadLogByMap(logMap, saveName, response);
                } else {
                    List<String> logList = hdfsManager.getSimpleLogByDate(date, module, memberId, env, level, src);
                    DownloadFileUtil.downloadLogByList(logList, saveName, response);
                }
            }
        } catch (IOException e){
            logger.error("error when downloading log: " + date + "! Caused by: " + e.getMessage());
        }
    }


}
