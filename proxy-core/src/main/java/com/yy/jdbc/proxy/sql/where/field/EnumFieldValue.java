package com.yy.jdbc.proxy.sql.where.field;

import java.util.*;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class EnumFieldValue<T extends Comparable<T>> implements FieldValue<T> {


	protected boolean not = false;
	protected List<Value<T>> values;

	public static <V extends Comparable<V>> EnumFieldValue<V> of(Value<V> ... value) {
		EnumFieldValue<V> fv = new EnumFieldValue<>();
		fv.values = Arrays.asList(value);
		Collections.sort(fv.values);
		return fv;
	}

	public static <V extends Comparable<V>> EnumFieldValue<V> ofNot(Value<V> ... value) {
		EnumFieldValue<V> fv = new EnumFieldValue<>();
		fv.values = Arrays.asList(value);
		fv.not = true;
		Collections.sort(fv.values);
		return fv;
	}

	@Override
	public boolean isRange() {
		return false;
	}

	@Override
	public Iterable<Range<T>> getRange() {
		return null;
	}

	@Override
	public Iterable<Value<T>> getValues() {
		return values;
	}

	@Override
	public Relation relationWith(FieldValue<T> fv) {
		if(fv.isRange()) {
			Iterator<Range<T>> rangesIt = fv.getRange().iterator();
			Iterator<Value<T>> valuesIt = values.iterator();
			Range<T> range = null;
			Value<T> value = null;
			while(true) {
				if(!rangesIt.hasNext() && null == range)
					return Relation.UNKNOWN;
				if(!valuesIt.hasNext() && null == value)
					return not ? Relation.CONTAINS : Relation.BELONGS;
				if(null == range)
					range = rangesIt.next();
				if(null == value)
					value = valuesIt.next();

				if(not ? !range.contains(value) : range.contains(value)) {
					value = null;
				} else {
					if(not)
						return Relation.UNKNOWN;
					range = null;
				}
			}
		} else {
			EnumFieldValue<T> efv = (EnumFieldValue<T>) fv;
//			if(this.values.size() != efv.values.size() && this.not == efv.not)
//				return Relation.UNKNOWN;

			if(this.not == efv.not) {
				if(values.size() > efv.values.size()) {
					if(values.containsAll(efv.values))
						return not ? Relation.BELONGS : Relation.CONTAINS;
				} else if(efv.values.size() > values.size()) {
					if(efv.values.containsAll(values))
						return not ? Relation.CONTAINS : Relation.BELONGS;
				} else {
					return values.containsAll(efv.values) ? Relation.EQUAL : Relation.UNKNOWN;
				}
				return Relation.UNKNOWN;
			} else {
				Collection<Value<T>> tmp = new HashSet<>(values);
				tmp.retainAll(efv.values);
				if(tmp.size() > 0)
					return Relation.UNKNOWN;

				return not ? Relation.CONTAINS: Relation.BELONGS;

			}
		}
	}
}
