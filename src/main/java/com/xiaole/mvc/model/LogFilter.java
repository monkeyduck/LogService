package com.xiaole.mvc.model;

import net.sf.json.JSONObject;

/**
 * Created by llc on 16/11/17.
 */
public class LogFilter {
    private String memberId;
    private String module;
    private String env;
    private String level;
    private boolean hasFilter;

    public LogFilter(String memberId, String module, String env, String level) {
        if (memberId.equals("all") && module.equals("all") && env.equals("all") && level.equals("all")) {
            hasFilter = false;
            this.memberId = null;
            this.module = null;
            this.env = null;
            this.level = null;
        }else{
            hasFilter = true;
            this.memberId = memberId;
            this.module = module;
            this.env = env;
            this.level = level;
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
                if (!json.getString("module").toLowerCase().equals(this.module.toLowerCase())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (!this.env.equals("all")) {
            if (json.containsKey("environment")) {
                if (!json.getString("environment").toLowerCase().equals(this.env.toLowerCase())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (!this.level.equals("all")) {
            if (json.containsKey("level")) {
                if (!json.getString("level").toLowerCase().equals(this.level.toLowerCase())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public boolean filter(NormalLog log) {
        if (!hasFilter) return true;
        if (!this.memberId.equals("all")) {
            if (!log.getMember_id().equals(this.memberId)) {
                return false;
            }
        }
        if (!this.module.equals("all")) {
            if (!log.getModtrans().toLowerCase().equals(this.module.toLowerCase())) {
                return false;
            }
        }
        if (!this.env.equals("all")) {
            if (!log.getEnvType().name().toLowerCase().equals(this.env.toLowerCase())) {
                return false;
            }
        }
        if (!this.level.equals("all")) {
            if (!log.getLevel().toLowerCase().equals(this.level.toLowerCase())) {
                return false;
            }
        }
        return true;
    }
}
