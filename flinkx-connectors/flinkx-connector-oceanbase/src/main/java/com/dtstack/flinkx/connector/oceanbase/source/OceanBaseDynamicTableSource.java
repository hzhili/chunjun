package com.dtstack.flinkx.connector.oceanbase.source;

import com.dtstack.flinkx.connector.oceanbase.conf.OceanBaseConf;
import com.dtstack.flinkx.connector.oceanbase.converter.OceanBaseRawConverter;
import com.dtstack.flinkx.connector.oceanbase.inputformat.OceanBaseInputFormatBuilder;
import com.dtstack.flinkx.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class OceanBaseDynamicTableSource implements ScanTableSource {
    private final TableSchema schema;
    private final OceanBaseConf conf;

    public OceanBaseDynamicTableSource(TableSchema physicalSchema, OceanBaseConf oceanBaseConf) {
        this.schema = physicalSchema;
        this.conf = oceanBaseConf;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        InternalTypeInfo<RowData> internalTypeInfo = InternalTypeInfo.of(rowType);
        OceanBaseInputFormatBuilder builder = new OceanBaseInputFormatBuilder();
        builder.setOceanBaseConfig(conf);
        builder.setRawConverter(
                new OceanBaseRawConverter((RowType) schema.toRowDataType().getLogicalType()));
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), internalTypeInfo), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new OceanBaseDynamicTableSource(schema, conf);
    }

    @Override
    public String asSummaryString() {
        return "OceanBaseDynamicTableSource:";
    }
}
