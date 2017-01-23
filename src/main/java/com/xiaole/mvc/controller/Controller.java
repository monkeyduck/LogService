package com.xiaole.mvc.controller;

import com.xiaole.elasticsearch.ELServer;
import com.xiaole.hdfs.HDFSManager;
import com.xiaole.redis.IKVStore;
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
                                            @RequestParam(value = "level", defaultValue = "all") String level) {
        long sts = 0;
        long ets = 0;
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
            return elServer.getLogBetweenTimeStamp(sts, ets, memberId, module, level);
    }

    @RequestMapping("/download/date")
    public void downloadLogByDate(HttpServletResponse response,
                                  @RequestParam("date") String date,
                                  @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                  @RequestParam(value = "module", defaultValue = "all") String module){
        try{
            String shortId = memberId;
            if (memberId.contains(".")) {
                shortId = memberId.split("\\.")[1];
            }
            String saveName = date + "_" + module + "_" + shortId + ".log";
            List<String> logList = hdfsManager.getLogByDate(date, module, memberId);
            hdfsManager.downloadLogByList(logList, saveName, response);
//            hdfsManager.downloadLogByDate(fileName, saveName, response);
        } catch (IOException e){
            logger.error("error when downloading log: " + date + "! Caused by: " + e.getMessage());
        }
    }

    @RequestMapping("/search/between-date")
    public String getLogBetweenDate(@RequestParam("startDate") String sdate,
                                  @RequestParam("endDate") String edate,
                                  @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                                  @RequestParam(value = "module", defaultValue = "all") String module,
                                  @RequestParam(value = "level", defaultValue = "all") String level) {
        DateTime sdt = new DateTime(sdate);
        DateTime edt = new DateTime(edate);
        int range = Days.daysBetween(sdt, edt).getDays();
        if (range < 0) {
            return "参数输入错误: 起始日期应小于截止日期";
        } else if (range == 0) {
            return elServer.getLogByDateFromELSearch(sdate, memberId, module, level);
        } else {
            return elServer.getLogBetweenDate(sdate, edate, range, memberId, module, level);
        }
    }


    @RequestMapping("/search/date")
    public String getLogByDate(@RequestParam("date") String date,
                               @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                               @RequestParam(value = "module", defaultValue = "all") String module,
                               @RequestParam(value = "level", defaultValue = "all") String level) {
        return elServer.getLogByDateFromELSearch(date, memberId, module, level);
    }


}
