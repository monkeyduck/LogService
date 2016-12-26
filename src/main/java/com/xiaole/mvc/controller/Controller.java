package com.xiaole.mvc.controller;

import com.xiaole.elasticsearch.ELServer;
import com.xiaole.hdfs.HDFSManager;
import com.xiaole.redis.IKVStore;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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
public class Controller {

    private static final Logger logger = Logger.getLogger(Controller.class);

    @Autowired
    private IKVStore redis;

    @Autowired
    private HDFSManager hdfsManager;

    @Autowired
    private ELServer elServer;

    @RequestMapping("/getLatestLog")
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

    @RequestMapping("/getLogBetweenTimeStamp")
    public JSONArray getLogBetweenTimeStamp(@RequestParam("startTimeStamp") String startTimeStamp,
                                            @RequestParam("endTimeStamp") String endTimeStamp) {
        List<String> logList = new ArrayList<>();
        try {
            long sTS = Long.parseLong(startTimeStamp);
            long eTS = Long.parseLong(endTimeStamp);
            DateTime dt = new DateTime(sTS);
            DateTime edt = new DateTime(eTS);
            System.out.println(dt.toString("yyyy-MM-dd HH:mm:ss.SSS"));
            System.out.println(edt.toString("yyyy-MM-dd HH:mm:ss.SSS"));
            List<String> firstMinute = redis.strLget("stats-" + dt.toString("yyyy-MM-dd.HH:mm"));
            int sindex = 0;
            JSONObject json;
            String slog;
            for (; sindex < firstMinute.size(); ++sindex) {
                slog = firstMinute.get(sindex);
                json = JSONObject.fromObject(slog);
                long ts = Long.parseLong(json.getString("timeStamp"));
                if (ts >= sTS) {
                    break;
                }
            }
            logList.addAll(firstMinute.subList(sindex, firstMinute.size()));
            dt = dt.plusMinutes(1);
            for (; dt.getMinuteOfDay() < edt.getMinuteOfDay(); dt = dt.plusMinutes(1)) {
                logList.addAll(redis.strLget("stats-" + dt.toString("yyyy-MM-dd.HH:mm")));
            }
            List<String> lastMinute = redis.strLget("stats-" + edt.toString("yyyy-MM-dd.HH:mm"));
            sindex = 0;
            for (; sindex < lastMinute.size(); ++sindex) {
                slog = lastMinute.get(sindex);
                json = JSONObject.fromObject(slog);
                long ts = Long.parseLong(json.getString("timeStamp"));
                if (ts <= eTS) {
                    logList.add(slog);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return JSONArray.fromObject(logList);
    }

    @RequestMapping("/downloadLogByDate")
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

    @RequestMapping("/getLogBetweenDate")
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
            return elServer.getLogBetweenDate(sdate, range, memberId, module, level);
        }
    }


    @RequestMapping("/getLogByDate")
    public String getLogByDate(@RequestParam("date") String date,
                               @RequestParam(value = "memberId", defaultValue = "all") String memberId,
                               @RequestParam(value = "module", defaultValue = "all") String module,
                               @RequestParam(value = "level", defaultValue = "all") String level) {
        return elServer.getLogByDateFromELSearch(date, memberId, module, level);
    }

}
