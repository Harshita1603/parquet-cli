package com.oracle.parquet.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;


public class JdbcUtils {
    
    public static Object extractResult(int sqlColummnType, ResultSet resultSet , int i) throws SQLException {
        switch (sqlColummnType) {
            case Types.BOOLEAN:
                return resultSet.getBoolean(i);
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.ROWID:
                return resultSet.getInt(i);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                return resultSet.getString(i);
            case Types.REAL:
            case Types.FLOAT:
                return resultSet.getFloat(i);
            case Types.DOUBLE:
                return resultSet.getDouble(i);
            case Types.NUMERIC:
                return resultSet.getBigDecimal(i);
            case Types.DECIMAL:
                return resultSet.getBigDecimal(i);
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                Calendar cal = Calendar.getInstance();
                cal.setTimeZone(TimeZone.getTimeZone("UTC"));
                java.sql.Timestamp ts = resultSet.getTimestamp(i,cal);
                return ts.getTime();
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case -101:
                return resultSet.getTimestamp(i).getTime();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.NCLOB:
            case Types.REF_CURSOR:
                return resultSet.getBytes(i);
            default:
                return resultSet.getString(i);
        }
    }
}