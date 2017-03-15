package com.xiaole.mvc.model.comparator;

import com.xiaole.mvc.model.NormalLog;

import java.util.Comparator;

/**
 * Created by llc on 17/3/1.
 */
public class LogComparator implements Comparator<NormalLog> {
    @Override
    public int compare(NormalLog o1, NormalLog o2) {
        return o1.getLog_time().compareTo(o2.getLog_time());
    }
}
