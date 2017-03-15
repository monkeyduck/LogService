package com.xiaole.utils;

import org.apache.log4j.Logger;

/**
 * Created by llc on 17/3/15.
 */
public class Utils {
    private static final Logger logger = Logger.getLogger(Utils.class);
    public static String decodeUrl(String url) {
        try {
            logger.info("To decode: " + url);
            url = url.replace(" ", "+");
            String ret = java.net.URLDecoder.decode(java.net.URLEncoder.encode(url, "UTF-8"), "UTF-8");
            logger.info("Decode result: " + ret);
            return ret;
        } catch (Exception e){
            logger.error("Decode url error, url: " + url);
            return "";
        }
    }
}
