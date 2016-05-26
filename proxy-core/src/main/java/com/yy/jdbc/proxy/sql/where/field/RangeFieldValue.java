package com.yy.jdbc.proxy.sql.where.field;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class RangeFieldValue<T extends Comparable<T>> implements FieldValue<T> {

	private List<Range<T>> ranges;

	public static <V extends Comparable<V>> RangeFieldValue<V> of(Range<V> ... ranges) {
		RangeFieldValue<V> fv = new RangeFieldValue<>();
		fv.ranges = Arrays.asList(ranges);
		Collections.sort(fv.ranges);
		return fv;
	}

	private RangeFieldValue() {

	}

	private RangeFieldValue(List<Range<T>> ranges) {
		this.ranges = ranges;
	}

	@Override
	public boolean isRange() {
		return true;
	}

	@Override
	public Iterable<Range<T>> getRange() {
		return ranges;
	}

	@Override
	public Iterable<Value<T>> getValues() {
		return null;
	}

	@Override
	public Relation relationWith(FieldValue<T> fv) {
		if(fv.isRange()) {
			RangeFieldValue<T> rfv = (RangeFieldValue<T>) fv;
			if(ranges.isEmpty() && rfv.ranges.isEmpty())
				return Relation.EQUAL;
			if(ranges.isEmpty())
				return Relation.BELONGS;
			if(rfv.ranges.isEmpty())
				return Relation.CONTAINS;

			Range<T> myOutRange = new Range<>(ranges.get(0).start, ranges.get(ranges.size() - 1).end);
			Range<T> theOutRange = new Range<>(rfv.ranges.get(0).start, rfv.ranges.get(rfv.ranges.size() -1).end);
			Relation outRelation = myOutRange.relationWith(theOutRange);
			if(outRelation == Relation.CONTAINS) {
				boolean contains = false;
				for(Range<T> theRange : rfv.ranges) {
					for(Range<T> myRange : ranges) {
						Relation relation = myRange.relationWith(theRange);
						if(relation == Relation.SEPARATE)
							continue;
						if(relation == Relation.CONTAINS || relation == Relation.EQUAL) {
							contains = true;
							break;
						}
						return Relation.UNKNOWN;
					}
					if(!contains)
						return Relation.UNKNOWN;
				}
				return Relation.CONTAINS;
			} else if(outRelation == Relation.BELONGS) {
				boolean belongs = false;
				for(Range<T> theRange : rfv.ranges) {
					for(Range<T> myRange : ranges) {
						Relation relation = myRange.relationWith(theRange);
						if(relation == Relation.SEPARATE)
							continue;
						if(relation == Relation.BELONGS || relation == Relation.EQUAL) {
							belongs = true;
							break;
						}
						return Relation.UNKNOWN;
					}
					if(!belongs)
						return Relation.UNKNOWN;
				}
				return Relation.BELONGS;
			} else if(outRelation == Relation.EQUAL) {
				if(ranges.size() != rfv.ranges.size()) {
					if(ranges.size() == 1 || rfv.ranges.size() == 1) {
						// todo: 假定range与range之间是有间隙的
						return Relation.UNKNOWN;
					}
				}
				RangeFieldValue<T> mySub = new RangeFieldValue<>(ranges.subList(0, ranges.size() -1));
				RangeFieldValue<T> theSub = new RangeFieldValue<>(rfv.ranges.subList(0, rfv.ranges.size() -1));
				Relation subRelation = mySub.relationWith(theSub);
				Relation rgRelation = ranges.get(ranges.size() -1).relationWith(rfv.ranges.get(rfv.ranges.size() - 1));
				if(subRelation == Relation.EQUAL)
					return rgRelation;
				if(rgRelation == Relation.EQUAL)
					return subRelation;
				if(subRelation != rgRelation)
					return Relation.UNKNOWN;
				return subRelation;
			}
			return Relation.UNKNOWN;
		} else {
			EnumFieldValue<T> efv = (EnumFieldValue<T>) fv;
			Iterator<Range<T>> rangesIt = ranges.iterator();
			Iterator<Value<T>> valuesIt = fv.getValues().iterator();
			Range<T> range = null;
			Value<T> value = null;
			while(true) {
				if(!rangesIt.hasNext() && null == range)
					return Relation.UNKNOWN;
				if(!valuesIt.hasNext() && null == value)
					return efv.not ? Relation.BELONGS : Relation.CONTAINS;
				if(null == range)
					range = rangesIt.next();
				if(null == value)
					value = valuesIt.next();

				if(efv.not ? !range.contains(value) : range.contains(value)) {
					value = null;
				} else {
					if(efv.not)
						return Relation.UNKNOWN;
					range = null;
				}
			}
		}
	}
}
