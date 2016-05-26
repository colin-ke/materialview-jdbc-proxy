package com.yy.jdbc.proxy.sql.where.logic;

import com.yy.jdbc.proxy.sql.where.Condition;
import com.yy.jdbc.proxy.sql.where.ResultUtil;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class And extends LogicCondition {

    public And(Condition cdt1, Condition cdt2) {
        super(cdt1, cdt2);
    }

    @Override
    protected boolean needFullContain() {
        return true;
    }

    @Override
    public int belongs(Condition condition) {

        if (condition instanceof LogicCondition) {
            LogicCondition compoundCondition = (LogicCondition) condition;
            Integer result = null;
            for (Condition cdt : compoundCondition.decompose()) {
                int tempRes = belongs(cdt);
                if (compoundCondition.needFullContain()) {
                    // AND
                    result = ResultUtil.and(result, tempRes);
                } else {
                    // OR
                    result = ResultUtil.or(result, tempRes);
                }
            }
            if (null == result) {
                // this is not gonna happen!
                throw new RuntimeException("unexpected null result");
            }
            return result;
        }

        // 只要有其中一个条件属于，则整体属于
        return ResultUtil.or(cdt1.belongs(condition), cdt2.belongs(condition));
    }

    @Override
    public String toSql() {
        return "(" + cdt1.toSql() + ") AND (" + cdt2.toSql() + ")";
    }
}
