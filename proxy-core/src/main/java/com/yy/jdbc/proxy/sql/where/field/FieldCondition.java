package com.yy.jdbc.proxy.sql.where.field;

import com.yy.jdbc.proxy.sql.Field;
import com.yy.jdbc.proxy.sql.Selectable;
import com.yy.jdbc.proxy.sql.where.Condition;
import com.yy.jdbc.proxy.sql.where.ResultUtil;
import com.yy.jdbc.proxy.sql.where.logic.And;
import com.yy.jdbc.proxy.sql.where.logic.Or;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author colin.ke keqinwu@yy.com
 */
@SuppressWarnings("unchecked")
public class FieldCondition<T extends Comparable<T>> implements Condition {

	private final Field field;

	private final FieldValue<T> fieldValue;

	private final String sql;

	private FieldCondition(Field field, FieldValue<T> fv, String sql) {
		this.field = field;
		this.fieldValue = fv;
		this.sql = sql;
	}

	@Override
	public int belongs(Condition condition) {
		if (condition instanceof And) {
			Condition[] subConditions = ((And) condition).decompose();
			return ResultUtil.and(this.belongs(subConditions[0]), this.belongs(subConditions[1]));
		} else if (condition instanceof Or) {
			Condition[] subConditions = ((Or) condition).decompose();
			return ResultUtil.or(this.belongs(subConditions[0]), this.belongs(subConditions[1]));
		} else if (condition instanceof FieldCondition) {
			if(!field.equals(((FieldCondition) condition).field))
				return -1;
			Relation fvRelation = fieldValue.relationWith(((FieldCondition) condition).fieldValue);
			if(fvRelation == Relation.BELONGS)
				return 1;
			else if (fvRelation == Relation.EQUAL)
				return 0;
			else
				return -1;
		} else {
			// should not go here.
			return -1;
		}
	}

	@Override
	public String toSql() {
		return field.toSql() + sql;
	}

	@Override
	public List<Selectable> getSelectable() {
		return Collections.singletonList((Selectable) field);
	}


	public static <V extends Comparable<V>> FieldCondition<V> notIn(Field field, V... value) {
		Value[] valArr = new Value[value.length];
		for (int i = 0; i < value.length; ++i) {
			valArr[i] = new Value(value[i]);
		}
		FieldValue<V> fv = EnumFieldValue.ofNot(valArr);
		return new FieldCondition<>(field, fv, (" NOT IN (" + StringUtils.join(value, ',') + ")"));
	}

	public static <V extends Comparable<V>> FieldCondition<V> in(Field field, V... value) {
		Value[] valArr = new Value[value.length];
		for (int i = 0; i < value.length; ++i) {
			valArr[i] = new Value(value[i]);
		}
		FieldValue<V> fv = EnumFieldValue.of(valArr);
		return new FieldCondition<>(field, fv, (" IN (" + StringUtils.join(value, ',') + ")"));
	}

	public static <V extends Comparable<V>> FieldCondition<V> notEq(Field field, V value) {
		FieldValue<V> fv = EnumFieldValue.ofNot(new Value<>(value));
		return new FieldCondition<>(field, fv, (" <> " + value));
	}

	public static <V extends Comparable<V>> FieldCondition<V> eq(Field field, V value) {
		FieldValue<V> fv = EnumFieldValue.of(new Value<>(value));
		return new FieldCondition<>(field, fv, (" = " + value));
	}


	public static <V extends Comparable<V>> FieldCondition<V> gt(Field field, V value) {
		FieldValue<V> fv = RangeFieldValue.of(new Range<V>(new Value<>(value, true), (Value<V>) Value.POSITIVE_INFINITY));
		return new FieldCondition<>(field, fv, (" > " + value));
	}

	public static <V extends Comparable<V>> FieldCondition<V> gte(Field field, V value) {
		FieldValue<V> fv = RangeFieldValue.of(new Range<V>(new Value<>(value), (Value<V>) Value.POSITIVE_INFINITY));
		return new FieldCondition<>(field, fv, (" >= " + value));
	}

	public static <V extends Comparable<V>> FieldCondition<V> lt(Field field, V value) {
		FieldValue<V> fv = RangeFieldValue.of(new Range<V>((Value<V>) Value.NEGATIVE_INFINITY, new Value<>(value, true)));
		return new FieldCondition<>(field, fv, (" < " + value));
	}

	public static <V extends Comparable<V>> FieldCondition<V> lte(Field field, V value) {
		FieldValue<V> fv = RangeFieldValue.of(new Range<V>((Value<V>) Value.NEGATIVE_INFINITY, new Value<>(value)));
		return new FieldCondition<>(field, fv, (" <= " + value));
	}
}
