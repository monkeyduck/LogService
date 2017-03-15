package com.xiaole.mvc.model.checker;

import com.xiaole.mvc.model.NormalLog;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

/**
 * Created by llc on 17/3/15.
 */
public class SdkLogChecker {
    private static final Logger logger = Logger.getLogger(SdkLogChecker.class);

    public static boolean check(NormalLog log) {
        return log.getModtrans().equals("cockroach");
    }

    public static boolean check(String slog) {
        try {
            JSONObject json = JSONObject.fromObject(slog);
            return json.getString("module").equals("cockroach");
        } catch (Exception e) {
            logger.error("parse to json error, log: " + slog);
            return false;
        }
    }
}
