package com.yy.jdbc.proxy.sql;

import com.yy.jdbc.proxy.sql.where.Condition;
import com.yy.jdbc.proxy.sql.where.field.Relation;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class SqlExpression implements Serializable {
    private List<Selectable> selects;
    private From from;
    private Condition condition;
    private Field[] groupBy;
    private OrderBy[] orderBy;
    private String limit;

    public SqlExpression(List<Selectable> selects, From from) {
        this.selects = selects;
        this.from = from;
    }

    public SqlExpression withCondition(Condition condition) {
        this.condition = condition;
        return this;
    }

    public SqlExpression withGroupBy(Field... fields) {
        groupBy = fields;
        return this;
    }

    public SqlExpression withOrderBy(OrderBy... fields) {
        orderBy = fields;
        return this;
    }

    public SqlExpression withLimit(String limit) {
        this.limit = limit;
        return this;
    }


    /**
     * 关于物化视图与目标sql的from关系
     * * 物化视图 完全包含 目标sql：where也包含，groupBy顺序相同，粒度比目标sql细，则可将目标的from直接改写成物化视图
     * * 物化视图 相交于   目标sql：相交部分where条件属于 物化视图的where条件，groupBy同上，则可将交集部分的from改写成物化视图的表
     * todo: 暂时只支持物化视图完全包含目标sql表达式的情况。
     *
     * @return
     */
    public SqlExpression tryRewrite(SqlExpression mtViewSqlEx, String mtViewName) {
        // from
        Relation fromRelation = from.relationWith(mtViewSqlEx.from);
        if (fromRelation != Relation.EQUAL && fromRelation != Relation.BELONGS)
            return this;

        Map<Selectable, Selectable> viewSelects = new HashMap<>();
        for (Selectable select : mtViewSqlEx.selects) {
            viewSelects.put(select, select);
        }

        // group by
        boolean sameGranularity = true;
        Field[] rewrittenGroupBy = null;
        if (null != groupBy) {

            sameGranularity = false;
            if (null != mtViewSqlEx.groupBy) {
                if (mtViewSqlEx.groupBy.length < groupBy.length)
                    return this; // 视图group粒度肯定比本sql表达式的粗，无法从粗粒度的数据中汇聚出更细粒度的，不属于

                Set<Field> mtGroupBySet = new HashSet<>(Arrays.asList(mtViewSqlEx.groupBy));
                for(Field f : groupBy) {
                    if(!mtGroupBySet.contains(f))
                        return this; // group by的字段不同
                    if(!viewSelects.containsKey(f))
                        return this; // 视图中的select不包含该group by字段
                }

                sameGranularity = mtViewSqlEx.groupBy.length == this.groupBy.length;
                rewrittenGroupBy = sameGranularity ? null : groupBy;
            } else {
                // 物化视图没有groupBy
                rewrittenGroupBy = groupBy;
            }
        } else {
            if (null != mtViewSqlEx.groupBy)
                return this; // 物化视图有groupBy，目标sql没有groupBy，无法从视图中查询

        }

        // where
        int whereRelation;
        if(mtViewSqlEx.condition == null && condition == null)
            whereRelation = 0;
        else {
            if(null != condition) {
                for(Selectable slt : condition.getSelectable()) {
                    if(!viewSelects.containsKey(slt))
                        return this; // 视图的select中不包含 where条件中的该的字段
                }
            }
            if (mtViewSqlEx.condition == null)
                whereRelation = 1;
            else if (condition == null)
                return this; // 物化视图有where，查询sql无where
            else
                whereRelation = condition.belongs(mtViewSqlEx.condition);
        }
        if (whereRelation < 0)
            return this; // where条件不属于

        //todo: 可以递归地把完全相同的子条件去掉。
        Condition rewritternCdt = whereRelation == 0 ? null : condition;

//        if(whereRelation >= 0) {
//            whereRelation = 0;
//        }

        // select
        Map<Selectable, Selectable> rwSelectMap = new HashMap<>();

        for (Selectable slt : selects) {
            if (!viewSelects.containsKey(slt)) {
                if (slt.isAgg() && viewSelects.containsKey(slt.getField())) {
                    // 物化视图的select中不包含该汇聚，但包含了汇聚列的明细
                    // 列明改成物化视图中的列明 如：sum(a.x) -> sum(a.x_mtv)
                    rwSelectMap.put(slt, slt.copy().rewrite(mtViewName, viewSelects.get(slt.getField()).aliasOrName(), false));
                } else {
                    // 目标sql表达式中不包含该select，不属于
                    return this;
                }
            } else {
                if(slt.isAgg()) {
                    // 是汇聚
                    if(slt.associative() && !sameGranularity) {
                        // where条件完全相同，且汇聚函数具有结合（可拆分）性，但groupBy粒度不相同
                        // 在已汇聚好的基础上继续汇聚
                        rwSelectMap.put(slt, slt.copy().rewrite(mtViewName, viewSelects.get(slt).aliasOrName(), true));
                    } else if (sameGranularity) {
                        // where条件完全相同，且粒度相同
                        // 直接select已汇聚好的
                        Field aggRewritten = new Field(mtViewName, viewSelects.get(slt).aliasOrName()).withAlias(slt.getAlias());
                        rwSelectMap.put(slt, aggRewritten);
                    } else {
                        // 对明细列进行汇聚
                        // 判断物化视图select中是否包含该汇聚中的明细列
                        if (!viewSelects.containsKey(slt.getField()))
                            return this; // 物化视图的select中不包含该汇聚中的明细列，无法从物化视图中查
                        rwSelectMap.put(slt, slt.copy().rewrite(mtViewName, viewSelects.get(slt.getField()).aliasOrName(), false));
                    }
                } else {
                    // 非汇聚
                    rwSelectMap.put(slt, slt.copy().rewrite(mtViewName, viewSelects.get(slt).aliasOrName(), false));
                }
            }
        }

        // for debug
//        for(Selectable slt : rwSelectMap.keySet()) {
//            System.out.println(slt.toSql() + "\t->\t" + rwSelectMap.get(slt).toSql());
//        }
        List<Selectable> rewrittenSelect = new ArrayList<>();
        for(Selectable slt : selects) {
            rewrittenSelect.add(rwSelectMap.get(slt));
        }

        // order by
        if(null != orderBy) {
            for (OrderBy anOrderBy : orderBy) {
                if (!viewSelects.containsKey(anOrderBy.getField()))
                    return this; // 目标数据集中不包含需要排序的列
            }
        }

        if (null != rewritternCdt) {
            for(Selectable slt : rewritternCdt.getSelectable())
                slt.rewrite(mtViewName, viewSelects.get(slt).aliasOrName(), false);
        }

        if (null != rewrittenGroupBy) {
            for(int i=0;i<rewrittenGroupBy.length;++i)
                rewrittenGroupBy[i] = (Field) rewrittenGroupBy[i].copy().rewrite(mtViewName, viewSelects.get(rewrittenGroupBy[i]).aliasOrName(), false);
        }

        return new SqlExpression(rewrittenSelect, From.tableWithoutJoin(mtViewName))
                .withCondition(rewritternCdt)
                .withGroupBy(rewrittenGroupBy)
                .withOrderBy(orderBy)
                .withLimit(limit);
    }

    public int priority(SqlExpression mtSql) {
        if(null == groupBy && mtSql.groupBy == null)
            return 0;
        if(mtSql.groupBy == null)
            return Integer.MAX_VALUE;
        if(null == groupBy)
            return mtSql.groupBy.length;
        return mtSql.groupBy.length - groupBy.length;
    }

    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(StringUtils.join(selects, ',')).append(System.lineSeparator());
        sql.append(from);
        if (null != condition) {
            sql.append(System.lineSeparator()).append("WHERE ").append(condition.toSql());
        }
        if (null != groupBy) {
            sql.append(System.lineSeparator()).append("GROUP BY ").append(StringUtils.join(groupBy, ','));
        }
        if (null != orderBy) {
            sql.append(System.lineSeparator()).append("ORDER BY ").append(StringUtils.join(orderBy, ','));
        }
        if (null != limit) {
            sql.append(System.lineSeparator()).append("LIMIT ").append(limit);
        }
        return sql.append(";").toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
