package com.xiaole.mvc.model.checker;

/**
 * Created by llc on 17/2/27.
 */
public class InvalidLogChecker{
    public static boolean check(String log) {
        return !("".equals(log));
    }
}
