package com.xiaole.mvc.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by hxx on 3/30/16.
 */
public enum EnvironmentType {
    Alpha,Beta,Online,Release;
    private static Map<String, EnvironmentType> dic = new HashMap<>();
    private static final String error_logStore = "logerror";
    private static final String error_logStore_alpha = "logerror-alpha";
    private static final String error_logStore_beta = "logerror-beta";
    private static final String online_logStore = "logstore";
    private static final String test_logStore = "logstore-test";
    private static final String beta_logStore = "logstore-beta";

    static {
        for (EnvironmentType type : values()) {
            String st = type.toString();
            dic.put(st.toLowerCase(), type);
        }
    }

    public static EnvironmentType fromString(String st) {
        Optional<EnvironmentType> type = Optional.ofNullable(dic.get(st.toLowerCase()));
        return type.orElse(EnvironmentType.Alpha);
    }


    public String logstoreError() {
        if (this == EnvironmentType.Alpha) {
            return error_logStore_alpha;
        }
        if (this == EnvironmentType.Beta) {
            return error_logStore_beta;
        }
        if (this == EnvironmentType.Online) {
            return error_logStore;
        }
        return error_logStore_alpha;
    }

    public String logstore() {
        if (this == EnvironmentType.Alpha) {
            return test_logStore;
        }
        if (this == EnvironmentType.Beta) {
            return beta_logStore;
        }
        if (this == EnvironmentType.Online) {
            return online_logStore;
        }
        return test_logStore;
    }
}
