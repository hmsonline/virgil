package com.hmsonline.virgil;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.simple.JSONObject;
import org.junit.Test;

import com.hmsonline.virgil.JsonMarshaller;

public class MarshallingTest {
    @Test
    public void testColumn() throws Exception {
        Column column = new Column(ByteBufferUtil.bytes("ADDR1"));
        column.setValue(ByteBufferUtil.bytes("1234 Fun St."));
        column.setTimestamp(System.currentTimeMillis() * 1000);
        ColumnOrSuperColumn col = new ColumnOrSuperColumn();
        col.setColumn(column);
        String json = JsonMarshaller.marshallColumn(col);
        assertEquals("{\"ADDR1\":\"1234 Fun St.\"}", json);
    }

    @Test
    public void testSlice() throws Exception {
        Column column1 = new Column(ByteBufferUtil.bytes("ADDR1"));
        column1.setValue(ByteBufferUtil.bytes("1234 Fun St."));
        column1.setTimestamp(System.currentTimeMillis() * 1000);
        ColumnOrSuperColumn col1 = new ColumnOrSuperColumn();
        col1.setColumn(column1);

        Column column2 = new Column(ByteBufferUtil.bytes("CITY"));
        column2.setValue(ByteBufferUtil.bytes("Souderton."));
        column2.setTimestamp(System.currentTimeMillis() * 1000);
        ColumnOrSuperColumn col2 = new ColumnOrSuperColumn();
        col2.setColumn(column2);

        List<ColumnOrSuperColumn> slice = new ArrayList<ColumnOrSuperColumn>();
        slice.add(col1);
        slice.add(col2);

        JSONObject json = JsonMarshaller.marshallSlice(slice);
        assertEquals(JSON.parse("{\"ADDR1\":\"1234 Fun St.\",\"CITY\":\"Souderton.\"}"), json);
    }
}
