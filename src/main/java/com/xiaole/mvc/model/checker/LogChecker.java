package com.xiaole.mvc.model.checker;

/**
 * Created by llc on 16/12/10.
 */
public class LogChecker {
    public LogChecker() {
    }

    public boolean check(String log) {
        return !("".equals(log));
    }
}
