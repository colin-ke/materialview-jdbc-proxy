package com.yy.jdbc.proxy.sql.where.field;

import java.io.Serializable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Value<T extends Comparable<T>> implements Comparable<Value<T>>, Serializable {

	public static Value NEGATIVE_INFINITY = new Value(null).withNINF(); //负无穷
	public static Value POSITIVE_INFINITY = new Value(null).withPINF(); //正无穷

	final T value;

	// 是否开区间
	final boolean opened;

	boolean isNINF = false;
	boolean isPINF = false;

	public Value(T value) {
		this.value = value;
		opened = false;
	}

	public Value(T value, boolean opened) {
		this.value = value;
		this.opened = opened;
	}

	private Value<T> withNINF() {
		isNINF = true;
		isPINF = false;
		return this;
	}

	private Value<T> withPINF() {
		isPINF = true;
		isNINF = false;
		return this;
	}

	@Override
	public int compareTo(Value<T> o) {
		if (this == o)
			return 0;

		if (isNINF && o.isNINF)
			return 0;

		if (isPINF && o.isPINF)
			return 0;

		if (isNINF || o.isPINF)
			return -1;

		if (isPINF || o.isNINF)
			return 1;

		int res = value.compareTo(o.value);

//		if(0 == res && opened != o.opened) {
//			if(opened)
//				res = -1;
//			else
//				res =  1;
//		}

		return res;
	}

	@Override
	public int hashCode() {
		if(null == value)
			return super.hashCode();
		else
			return value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return !(null == obj || !(obj instanceof Value)) && compareTo((Value<T>) obj) == 0;
	}

	@Override
	public String toString() {
		return value.toString();
	}

	public boolean isOpened() {
		return opened;
	}
}