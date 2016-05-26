package com.yy.jdbc.proxy.sql.where.field;

import java.io.Serializable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Range<T extends Comparable<T>> implements Comparable<Range<T>>, Serializable {
	Value<T> start;
	Value<T> end;

	public Range(Value<T> start, Value<T> end) {
		this.start = start;
		this.end = end;
	}

	public Relation relationWith(Range<T> o) {
		if (cmpVal(start, o.start, true, true) == 0 && cmpVal(end, o.end, false, false) == 0) {
			return Relation.EQUAL;
		}

		if (cmpVal(start, o.start, true, true) <= 0 && cmpVal(end, o.end, false, false) >= 0)
			return Relation.CONTAINS;

		if (cmpVal(start, o.start, true, true) >= 0 && cmpVal(end, o.end, false, false) <= 0)
			return Relation.BELONGS;

		if (o.start.compareTo(start) > 0 && cmpVal(o.start, end, true, false) <= 0)
			return Relation.CROSS;

		if (cmpVal(o.end, start, false, true) >= 0 && cmpVal(o.end, end, false, false) < 0)
			return Relation.CROSS;

		return Relation.SEPARATE;
	}

	private <V extends Comparable<V>> int cmpVal(Value<V> v1, Value<V> v2, boolean v1Start, boolean v2Start) {
		int res = v1.compareTo(v2);
		if (0 == res && v1.isOpened() != v2.isOpened()) {
			if (v1Start != v2Start)
				return v1Start ? 1 : -1;
			else {
				return v1.isOpened() ? -1 : 1;
			}
		}
		return res;
	}

	public boolean contains(Value<T> value) {
		int startRes = value.compareTo(start);
		int endRes = value.compareTo(end);

		if (startRes == 0 && start.isOpened())
			return false;

		if (endRes == 0 && end.isOpened())
			return false;

		return startRes >= 0 && endRes <= 0;
	}

	public Value<T> getStart() {
		return start;
	}

	public Value<T> getEnd() {
		return end;
	}

	@Override
	public int compareTo(Range<T> o) {
		return start.compareTo(o.start);
	}
}