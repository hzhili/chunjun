package com.dtstack.flinkx.connector.oceanbase.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.oceanbase.converter.OceanBaseRawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class OceanBaseSourceFactory extends SourceFactory {

    public OceanBaseSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return OceanBaseRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        return null;
    }
}
