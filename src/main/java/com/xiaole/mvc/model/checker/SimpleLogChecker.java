package com.xiaole.mvc.model.checker;

import com.xiaole.mvc.model.NormalLog;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

/**
 * Created by llc on 17/2/27.
 */
public class SimpleLogChecker {
    private static final Logger logger = Logger.getLogger(SimpleLogChecker.class);

    public static boolean check(String log) {
        if (SdkLogChecker.check(log)) {
            JSONObject json = JSONObject.fromObject(log);
            if (json.getString("module").equals("cockroach")) {
                if (json.getString("from").equals("frontend")
                        || json.getString("to").equals("frontend")) {
                    return true;
                }
            }
            return false;
        } else {
            try {
                NormalLog nLog = new NormalLog(log);
                return nLog.belongsToSimple();

            } catch (Exception e) {
                logger.error("parse log to NormalLog error, log: " + log);
                return false;
            }
        }
    }

    public static boolean check(NormalLog log) {
        return log.getModtrans().equals("cockroach");
    }
}
