package com.yy.jdbc.proxy.util;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.sql.SqlExpression;
import org.junit.Before;
import org.junit.Test;

/**
 * 序列化工具测试类
 *
 * @author ZhangXueJun
 */
public class SerializeHelperTest {


    static byte[] bytes = new byte[2048];

    @Before
    public void testSerialize() {

        MaterialView materialView = new MaterialView();
        materialView.setViewName("mv_test07");
        materialView.setSqlStr("select * from audit_privilege");
        materialView.setRewrite(true);
        materialView.setRefresh(MaterialView.Refresh.complete);
        materialView.setBuild(MaterialView.Build.immediate);
        materialView.setSqlDataStruct(new SqlExpression(null, null));
        materialView.setFactTable("audit_privilege");

        bytes = SerializeHelper.serialize(materialView);
    }

    @Test
    public void testDeserialize() {
        MaterialView materialView = SerializeHelper.deserialize(bytes);
        System.out.println();
    }

}
