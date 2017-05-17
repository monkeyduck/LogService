package com.xiaole.mvc.model.checker.simpleChecker;

/**
 * Created by llc on 17/3/24.
 */
public class SimpleCheckerFactory {
    public static CheckSimpleInterface genSimpleChecker(String src) {
        if (src.equals("xiaole")) {
            return new XiaoleSimpleChecker();
        } else {
            return new BevaSimpleChecker();
        }
    }
}
