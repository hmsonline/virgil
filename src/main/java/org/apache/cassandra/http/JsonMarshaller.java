package org.apache.cassandra.http;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.json.simple.JSONObject;

public class JsonMarshaller
{

    @SuppressWarnings("unchecked")
	public static String marshallColumn(ColumnOrSuperColumn column)
            throws UnsupportedEncodingException
    {
        JSONObject json = new JSONObject();
        Column c = column.getColumn();
        json.put(string(c.getName()), string(c.getValue()));
        return json.toString();
    }

    @SuppressWarnings("unchecked")
	public static String marshallSlice(List<ColumnOrSuperColumn> slice)
            throws UnsupportedEncodingException
    {
        JSONObject json = new JSONObject();
        for (ColumnOrSuperColumn column : slice)
        {
            Column c = column.getColumn();
            json.put(string(c.getName()), string(c.getValue()));
        }
        return json.toString();
    }

    private static String string(byte[] bytes) throws UnsupportedEncodingException
    {   
        return new String(bytes, "UTF8");
    }

}
