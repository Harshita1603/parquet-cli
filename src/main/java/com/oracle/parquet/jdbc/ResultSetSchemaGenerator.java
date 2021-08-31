package com.oracle.parquet.jdbc;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Parses a {@link ResultSet } and generates {@link SchemaResults } which contains the mappings of sql columns
 * to Schema fields.
 */
public class ResultSetSchemaGenerator {

    /**
     * Generates parquet schema using {@link ResultSetMetaData }
     *
     * @param resultSetMetaData
     * @param name Record name.
     * @param nameSpace
     * @return 
     * @throws SQLException
     */
    public Schema generateSchema(ResultSetMetaData resultSetMetaData, String name) throws SQLException {
        Schema recordSchema = Schema.createRecord(name, "Schema Created from JDBC Metadata", null, false);

        List<Schema.Field> fields = new ArrayList<>();
        int columnCount = resultSetMetaData.getColumnCount();

        for (int x = 1; x <= columnCount; x++) {

            String columnName = resultSetMetaData.getColumnLabel(x);
            int sqlColumnType = resultSetMetaData.getColumnType(x);
            Schema.Type schemaType = parseSchemaType(sqlColumnType);
            Boolean makeNullable = false;
            if(resultSetMetaData.isNullable(x)==resultSetMetaData.columnNullable)
                makeNullable = true;
            Schema temp = Schema.create(schemaType == null ? Schema.Type.STRING : schemaType); 
            temp = Logicalschema(sqlColumnType,temp);
            temp = schema(temp,makeNullable,schemaType);
            fields.add(new Schema.Field(columnName,temp,null,null));
        }

        recordSchema.setFields(fields);
        return recordSchema;
    }

    /**
     * Creates field that can be null {@link Schema#createUnion(List)}
     *
     * @param type         Schema type for this field
     * @param makeNullable set true if u want the field to be nullable
     * @return
     */
    private static Schema schema(Schema schema, boolean makeNullable,Schema.Type type) {
    
        if (makeNullable || type == null) {
          schema = Schema.createUnion(Lists.newArrayList(
              Schema.create(Schema.Type.NULL), schema));
        }
        return schema;
    }

    private static Schema Logicalschema(int sqlColumnType,Schema schema) {
        switch(sqlColumnType){
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.DATE:
            case -101:   
                LogicalTypes.timestampMillis().addToSchema(schema);break;
            
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                LogicalTypes.timeMillis().addToSchema(schema);break;
        }
        return schema;
    }

    /**
     * Converts sql column type to schema type. See {@link java.sql.Types } and {@link Schema.Type } for more details.
     *
     * https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
     *
     * @param sqlColumnType
     * @return
     */
    public Schema.Type parseSchemaType(int sqlColumnType) {
        switch (sqlColumnType) {

            case Types.BOOLEAN:
                return Schema.Type.BOOLEAN;

            case Types.TINYINT:             // 1 byte
            case Types.SMALLINT:            // 2 bytes
            case Types.INTEGER:             // 4 bytes
                return Schema.Type.INT;     // 32 bit (4 bytes) (signed)

            case Types.ROWID:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                return Schema.Type.STRING;  // unicode string

            case Types.REAL:                // Approximate numerical (mantissa single precision 7)
                return Schema.Type.FLOAT;   // A 32-bit IEEE single-float

            case Types.DOUBLE:              // Approximate numerical (mantissa precision 16)
            case Types.DECIMAL:             // Exact numerical (5 - 17 bytes)
            case Types.NUMERIC:             // Exact numerical (5 - 17 bytes)
            case Types.FLOAT:               // Approximate numerical (mantissa precision 16)
                return Schema.Type.DOUBLE;  // A 64-bit IEEE double-float

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.BIGINT:       
            case -101:       // 8 bytes
                return Schema.Type.LONG;    // 64 bit (signed)

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
                return Schema.Type.BYTES;   // sequence of bytes
        }

        return null;
    } 
}
