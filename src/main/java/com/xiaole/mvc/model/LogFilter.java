package com.xiaole.mvc.model;

import net.sf.json.JSONObject;

/**
 * Created by llc on 16/11/17.
 */
public class LogFilter {
    private String memberId;
    private String module;
    private boolean hasFilter;

    public LogFilter(String memberId, String module) {
        if (memberId.equals("all") && module.equals("all")) {
            hasFilter = false;
            this.memberId = null;
            this.module = null;
        }else{
            hasFilter = true;
            this.memberId = memberId;
            this.module = module;
        }
    }

    public boolean isHasFilter() {
        return hasFilter;
    }

    public boolean filter(String log) {
        if (!hasFilter) return true;
        JSONObject json = JSONObject.fromObject(log);
        if (!this.memberId.equals("all")) {
            if (json.containsKey("memberId")) {
                if (!json.getString("memberId").equals(this.memberId)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (!this.module.equals("all")) {
            if (json.containsKey("module")) {
                if (!json.getString("module").equals(this.module)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}
