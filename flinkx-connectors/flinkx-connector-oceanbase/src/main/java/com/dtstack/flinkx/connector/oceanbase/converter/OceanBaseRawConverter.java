package com.dtstack.flinkx.connector.oceanbase.converter;

import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Maps;
import com.oceanbase.oms.logmessage.LogMessage;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class OceanBaseRawConverter extends AbstractCDCRowConverter<LogMessage, LogicalType> {

    public OceanBaseRawConverter(RowType rowType) {
        super.fieldNameList = rowType.getFieldNames();
        super.converters = new ArrayList<>();
        rowType.getFields()
                .forEach(field -> super.converters.add(createInternalConverter(field.getType())));
    }

    @Override
    public LinkedList<RowData> toInternal(LogMessage input) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        HashMap<Object, Object> beforeMap =
                Maps.newHashMapWithExpectedSize(input.getFieldList().size());
        HashMap<Object, Object> afterMap =
                Maps.newHashMapWithExpectedSize(input.getFieldList().size());
        input.getFieldList()
                .forEach(
                        field -> {
                            if (field.isPrev()) {
                                beforeMap.put(field.getFieldname(), field.getValue().toString());
                            } else {
                                afterMap.put(field.getFieldname(), field.getValue().toString());
                            }
                        });
        switch (input.getOpt()) {
            case INSERT:
                RowData insert = createRowDataByConverters(fieldNameList, converters, afterMap);
                insert.setRowKind(RowKind.INSERT);
                result.add(insert);
                break;
            case DELETE:
                RowData delete = createRowDataByConverters(fieldNameList, converters, beforeMap);
                delete.setRowKind(RowKind.DELETE);
                result.add(delete);
                break;
            case UPDATE:
                RowData updateBefore =
                        createRowDataByConverters(fieldNameList, converters, beforeMap);
                updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                result.add(updateBefore);
                RowData updateAfter =
                        createRowDataByConverters(fieldNameList, converters, afterMap);
                updateAfter.setRowKind(RowKind.UPDATE_AFTER);
                result.add(updateAfter);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + input.getOpt());
        }
        return result;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>)
                        field -> StringData.fromString(field);
            case BOOLEAN:
                return (IDeserializationConverter<String, Boolean>) Boolean::getBoolean;
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<String, byte[]>)
                        field -> field.getBytes(StandardCharsets.UTF_8);
            case DECIMAL:
                return (IDeserializationConverter<String, DecimalData>)
                        field -> {
                            int precision = ((DecimalType) type).getPrecision();
                            int scale = ((DecimalType) type).getScale();
                            return DecimalData.fromBigDecimal(
                                    new BigDecimal(field), precision, scale);
                        };
            case TINYINT:
                return (IDeserializationConverter<String, Byte>) Byte::parseByte;
            case SMALLINT:
                return (IDeserializationConverter<String, Short>) Short::parseShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<String, Integer>) Integer::parseInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (IDeserializationConverter<String, Long>) Long::parseLong;
            case FLOAT:
                return (IDeserializationConverter<String, Float>) Float::parseFloat;
            case DOUBLE:
                return (IDeserializationConverter<String, Double>) Double::parseDouble;
            case DATE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            LocalDate date =
                                    DateTimeFormatter.ISO_LOCAL_DATE
                                            .parse(val)
                                            .query(TemporalQueries.localDate());
                            return (int) date.toEpochDay();
                        };
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            LocalTime localTime =
                                    SQL_TIME_FORMAT.parse(val).query(TemporalQueries.localTime());
                            return localTime.toSecondOfDay() * 1000;
                        };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (IDeserializationConverter<String, TimestampData>)
                        val -> TimestampData.fromTimestamp(Timestamp.valueOf(val));
            case NULL:
                return val -> null;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
