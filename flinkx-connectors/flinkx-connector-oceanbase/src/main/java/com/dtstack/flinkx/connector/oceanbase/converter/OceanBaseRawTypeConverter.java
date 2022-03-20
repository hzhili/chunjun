package com.dtstack.flinkx.connector.oceanbase.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class OceanBaseRawTypeConverter {

    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
            case "NUMBER":
                return DataTypes.BIGINT();
            case "DECIMAL":
                return DataTypes.DECIMAL(38, 30);
            case "FLOAT":
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();
            case "BIT":
                return DataTypes.BYTES();
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "TIME":
                return DataTypes.TIME();
            case "CHAR":
            case "NCHAR":
            case "VARCHAR":
            case "VARCHAR2":
            case "NVARCHAR2":
            case "BLOB":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "RAW":
            case "CLOB":
            case "INTERVAL YEAR TO MONTH":
            case "INTERVAL DAY TO SECOND":
            case "YEAR":
                return DataTypes.STRING();
            case "TIMESTAMP WITH TIME ZONE":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            default:
                throw new UnsupportedOperationException(type);
        }
    }
}
