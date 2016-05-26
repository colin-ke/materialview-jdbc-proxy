package com.yy.jdbc.proxy.sql;

import com.yy.jdbc.proxy.sql.where.field.Relation;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class From implements RelationComparable<From> {

	private final String[] tables;
	private final String[] onFields;

	public From(String[] tables, String[] onFields) {
		this.tables = tables;
		this.onFields = onFields;
	}

	public static From tableWithoutJoin(String tableName) {
		return new From(new String[]{tableName}, null);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("FROM ");
//		assert tables.length >= 1 : "from's tables should not be empty";
		sb.append(tables[0]);
		if (null != onFields && onFields.length > 0) {
//			assert tables.length == onFields.length + 1;
			for (int i = 1; i < tables.length; ++i) {
				sb.append(" inner join ").append(tables[i]).append(" on ").append(onFields[i - 1]);
			}
		}
		return sb.toString();
	}

	@Override
	public Relation relationWith(From obj) {
		if (null == obj)
			return Relation.UNKNOWN;
		if (this == obj)
			return Relation.EQUAL;
		if (!this.tables[0].equals(obj.tables[0]))
			return Relation.UNKNOWN;

		Set<String> thisSet = new HashSet<>();
		if(null != onFields)
			thisSet.addAll(Arrays.asList(onFields));
		Set<String> theSet = new HashSet<>();
		if(null != obj.onFields)
			theSet.addAll(Arrays.asList(obj.onFields));
		boolean flag = true;
		for (String str : theSet) {
			if (!thisSet.contains(str)) {
				flag = false;
				break;
			}
		}
		if (flag) {
			if (thisSet.size() == theSet.size())
				return Relation.EQUAL;
			else
				return Relation.CONTAINS;
		}

		flag = true;
		for (String str : thisSet) {
			if (!theSet.contains(str)) {
				flag = false;
				break;
			}
		}
		if (flag) {
			return Relation.BELONGS;
		}
		return Relation.UNKNOWN;
	}
}
