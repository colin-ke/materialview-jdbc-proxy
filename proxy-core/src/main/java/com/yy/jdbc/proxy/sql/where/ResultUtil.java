package com.yy.jdbc.proxy.sql.where;

/**
 * @auther colin.
 */
public class ResultUtil {

    private ResultUtil() {

    }

    public static Integer and(Integer result1, Integer result2) {
        if(null == result1 && null == result2)
            return null;
        if(null == result1)
            return result2;
        if(null == result2)
            return result1;

        if(result1 < 0 || result2 < 0)
            return -1;
        if(result1 + result2 == 0)
            return 0;
        return 1;
    }

    public static Integer or(Integer result1, Integer result2) {
        if(null == result1 && null == result2)
            return null;
        if(null == result1)
            return result2;
        if(null == result2)
            return result1;

        if(result1 < 0 && result2 < 0)
            return -1;
        if(result1 == 0 && result2 == 0)
            return 0;
        return 1;
    }
}
