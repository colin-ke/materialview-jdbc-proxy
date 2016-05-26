package com.yy.jdbc.proxy.sql.where;

import com.yy.jdbc.proxy.sql.Field;
import com.yy.jdbc.proxy.sql.where.field.*;
import com.yy.jdbc.proxy.sql.where.logic.And;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author colin_ke keqinwu@163.com
 */
public class ConditionTest {

	Value vn = Value.NEGATIVE_INFINITY;
	Value vp = Value.POSITIVE_INFINITY;
	Value<Long> v1 = new Value<>(-100l);
	Value<Long> v2 = new Value<>(0l);
	Value<Long> v3 = new Value<>(10l);
	Value<Long> v30 = new Value<>(10l, true);
	Value<Long> v4 = new Value<>(50l);
	Value<Long> v5 = new Value<>(60l);
	Value<Long> v6 = new Value<>(70l);
	Value<Long> v7 = new Value<>(80l);
	Value<Long> v8 = new Value<>(100l);
	Value<Long> v80 = new Value<>(100l, true);
	Value<Long> v9 = new Value<>(500l);

	Field field1 = new Field("table1", "field1");
	Field field10 = new Field("table1", "field1");
	Field field2 = new Field("table1", "field2");
	Field field20 = new Field("table1", "field2");

	@Test
	public void valueTest() {
		Value<String> v1 = new Value<>("abc");
		Value<String> v2 = new Value<>("bcd");
		Value<String> v3 = new Value<>("abc");
		Value<String> v4 = Value.NEGATIVE_INFINITY;
		Value<String> v5 = Value.POSITIVE_INFINITY;
		Assert.assertTrue(v1.compareTo(v2) < 0);
		Assert.assertTrue(v2.compareTo(v1) > 0);
		Assert.assertTrue(v3.compareTo(v1) == 0);
		Assert.assertTrue(v4.compareTo(v2) < 0);
		Assert.assertTrue(v5.compareTo(v2) > 0);

		Value<Long> v6 = new Value<>(100l);
		Value<Long> v7 = new Value<>(100l, true);
		System.out.println(v6.compareTo(v7));

	}

	@Test
	public void RangeTest() {

		Range<Long> range1 = new Range<>(vn, v8);
		Range<Long> range2 = new Range<>(v1, v8);
		Range<Long> range3 = new Range<>(v5, v9);
		Range<Long> range4 = new Range<>(v80, vp);


		System.out.println(range1.relationWith(range2)); // CONTAINS
		System.out.println(range2.relationWith(range1)); // BELONGS
		System.out.println(range1.relationWith(range3)); // CROSS
		System.out.println(range1.relationWith(range4)); // SEPARATE
		System.out.println(range4.relationWith(range1)); // SEPARATE
	}

	@Test
	public void FieldValueTest() {
		Range<Long> range1 = new Range<>(vn, v3);
		Range<Long> range2 = new Range<>(v30, v8);
		Range<Long> range20 = new Range<>(v30, v8);
		Range<Long> range3 = new Range<>(v5, v8);
		Range<Long> range4 = new Range<>(v80, vp);

		EnumFieldValue<Long> efv1 = EnumFieldValue.of(v1, v2, v3);
		EnumFieldValue<Long> efv2 = EnumFieldValue.ofNot(v1, v2, v3);
		EnumFieldValue<Long> efv3 = EnumFieldValue.of(v1, v2, v3, v4, v5);
		EnumFieldValue<Long> efv4 = EnumFieldValue.of(v5, v6, v7, v8, v9);

		RangeFieldValue<Long> rfv1 = RangeFieldValue.of(range1);
		RangeFieldValue<Long> rfv2 = RangeFieldValue.of(range2);
		RangeFieldValue<Long> rfv24 = RangeFieldValue.of(range2, range4);
		RangeFieldValue<Long> rfv204 = RangeFieldValue.of(range20, range4);
		RangeFieldValue<Long> rfv12 = RangeFieldValue.of(range1, range2);
		RangeFieldValue<Long> rfv3 = RangeFieldValue.of(range3);
		RangeFieldValue<Long> rfv4 = RangeFieldValue.of(range4);
		RangeFieldValue<Long> rfv34 = RangeFieldValue.of(range3, range4);

		System.out.println(efv1.relationWith(efv2)); // UNKNOWN
		System.out.println(efv1.relationWith(efv3)); // BELONGS
		System.out.println(efv3.relationWith(efv1)); // CONTAINS
		System.out.println();
		System.out.println(rfv1.relationWith(efv1)); // CONTAINS
		System.out.println(efv1.relationWith(rfv1)); // BELONGS
		System.out.println(rfv2.relationWith(efv2)); // BELONGS
		System.out.println(efv2.relationWith(rfv2)); // CONTAINS
		System.out.println();
		System.out.println(rfv12.relationWith(efv3)); // CONTAINS
		System.out.println(efv3.relationWith(rfv12)); // BELONGS
		System.out.println(rfv12.relationWith(efv2)); // UNKNOWN
		System.out.println(efv2.relationWith(rfv12)); // UNKNOWN
		System.out.println(rfv34.relationWith(efv4)); // CONTAINS
		System.out.println(efv4.relationWith(rfv34)); // BELONGS
		System.out.println();
		System.out.println(rfv24.relationWith(rfv3)); // CONTAINS
		System.out.println(rfv3.relationWith(rfv24)); // BELONGS
		System.out.println(rfv204.relationWith(rfv3)); // CONTAINS
		System.out.println(rfv3.relationWith(rfv204)); // BELONGS
		System.out.println(RangeFieldValue.of(range2).relationWith(RangeFieldValue.of(range20))); // EQUAL
	}

	@Test
	public void fieldCdtTest() {
		System.out.println(FieldCondition.eq(field1, v3).belongs(FieldCondition.eq(field1, v3))); // 0
		System.out.println(FieldCondition.eq(field1, v3).belongs(FieldCondition.eq(field2, v3))); // -1
		System.out.println();
		System.out.println(FieldCondition.gt(field1, v3).belongs(FieldCondition.gt(field1, v2))); // 1
		System.out.println(FieldCondition.gte(field1, v3).belongs(FieldCondition.gt(field1, v3))); // -1
		System.out.println(FieldCondition.gt(field1, v3).belongs(FieldCondition.gte(field1, v3))); // 1
		System.out.println(FieldCondition.gt(field1, v3).belongs(FieldCondition.gt(field1, v4))); // -1
		System.out.println();
		System.out.println(FieldCondition.in(field1, v1, v2, v3).belongs(FieldCondition.in(field1, v1, v2, v3))); // 0
		System.out.println(FieldCondition.in(field1, v1, v2, v3).belongs(FieldCondition.in(field1, v1, v2, v3, v4))); // 1
		System.out.println(FieldCondition.notEq(field1, v1).belongs(FieldCondition.notEq(field1, v2))); // -1
		System.out.println(FieldCondition.notEq(field1, v1).belongs(FieldCondition.notEq(field1, v1))); // 0
		System.out.println();
		System.out.println(FieldCondition.lt(field1, v8).belongs(FieldCondition.gte(field1, v7))); // -1
	}

	@Test
	public void logicCdtTest() {
		FieldCondition fc11 = FieldCondition.gt(field1, v1);
		FieldCondition fc12 = FieldCondition.lt(field1, v3);
		FieldCondition fc13 = FieldCondition.gte(field1, v2);
		FieldCondition fc14 = FieldCondition.lte(field1, v3);
		FieldCondition eq2 = FieldCondition.eq(field2, v2);

		And and1 = new And(fc12, fc11); // v1 < f1 < v3
		And and2 = new And(fc13, fc14); // v2 <= f1 <= v3
		And and20 = new And(fc13, fc12);  // v2 <= f1 < v3
		And and3 = new And(and1, eq2); // (v1 < f1 < v3) and f2=v2

		System.out.println(and1.belongs(fc14)); // 1
		System.out.println(and2.belongs(and1)); // -1
		System.out.println(and1.belongs(and2)); // 1
		System.out.println(and20.belongs(and1)); // 1
		System.out.println();
		System.out.println(and3.belongs(and1)); // 1
		System.out.println(and1.belongs(and3)); // -1


	}

















}
