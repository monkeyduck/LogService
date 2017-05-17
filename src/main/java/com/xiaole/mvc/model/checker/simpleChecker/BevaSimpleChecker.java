package com.xiaole.mvc.model.checker.simpleChecker;

import com.xiaole.mvc.model.checker.SdkLogChecker;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

/**
 * Created by llc on 17/3/24.
 */
public class BevaSimpleChecker implements CheckSimpleInterface {
    private static final Logger logger = Logger.getLogger(BevaSimpleChecker.class);

    @Override
    public boolean checkSimple(String log) {
        if (SdkLogChecker.check(log)) {
            JSONObject json = JSONObject.fromObject(log);
            if (json.getString("module").equals("cockroach")) {
                if (json.getString("from").equals("frontend")
                        || json.getString("to").equals("frontend")) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public static void main(String[] args) {
        String line = "{\"content\":\"{\\\"cmd\\\":\\\"QA\\\",\\\"memberI      d\\\":\\\"af4b8ee8d3c94ff599e2877224ee3e130.85867965\\\",\\\"replyModule\\\":\\\"GeneralGame\\\",\\\"to\\\":\\\"us      er\\\",\\\"link\\\":\\\"GeneralGame\\\",\\\"replyContent\\\":\\\"clear_teacher_animation\\\",\\\"replyType\\\":\\\"ani      mation\\\",\\\"replyConfidence\\\":1,\\\"replyVoiceRole\\\":\\\"child\\\",\\\"packageId\\\":1491521004230,\\\"devi      ceId\\\":\\\"18:97:ff:0a:80:f0\\\"}\",\"deviceId\":\"18:97:ff:0a:80:f0\",\"memberId\":\"af4b8ee8d3c94ff599e2      877224ee3e130.85867965\",\"environment\":\"release\",\"level\":\"INFO\",\"ip\":\"10.25.115.47\",\"timeStamp\"      :1491521004276,\"module\":\"GeneralGame->user\",\"@version\":\"1\",\"@timestamp\":\"2017-04-07T07:23:24.2      76+08:00\",\"path\":\"/home/zhangyuxiao/stats_logs/2017-04-07.log\",\"host\":\"iZ25841et8bZ\",\"type\":\"s      tats\",\"debug\":\"timestampMatched\",\"hdfs_filename\":\"2017-04-07\",\"redis_key\":\"2017-04-07.07:23\"}\n";
        CheckSimpleInterface checker = SimpleCheckerFactory.genSimpleChecker("beiwa");
        System.out.println(checker.checkSimple(line));
    }
}
