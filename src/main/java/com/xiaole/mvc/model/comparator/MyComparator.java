package com.xiaole.mvc.model.comparator;

import java.util.Comparator;

/**
 * Created by llc on 17/3/1.
 */
public class MyComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
        return o2 - o1;
    }
}
