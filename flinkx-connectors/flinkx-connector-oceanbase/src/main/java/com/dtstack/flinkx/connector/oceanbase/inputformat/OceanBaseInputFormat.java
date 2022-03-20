package com.dtstack.flinkx.connector.oceanbase.inputformat;

import com.dtstack.flinkx.connector.oceanbase.conf.OceanBaseConf;
import com.dtstack.flinkx.connector.oceanbase.listener.OceanBaseRecordListener;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;

import java.io.IOException;

public class OceanBaseInputFormat extends BaseRichInputFormat {
    public OceanBaseConf oceanBaseConf;
    public OceanBaseRecordListener recordListener;
    public LogProxyClient logProxyClient;
    public Long lastSyncTimestamp;
    private AbstractCDCRowConverter rowConverter;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return new InputSplit[] {new GenericInputSplit(1, 1)};
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        lastSyncTimestamp = getLastSyncTimestamp();
    }

    public Long getLastSyncTimestamp() {
        if (null != formatState && formatState.getState() != null) {
            return Long.parseLong(formatState.getState().toString());
        }
        return 0L;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(lastSyncTimestamp);
        }
        return formatState;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        ObReaderConfig readerConfig = new ObReaderConfig();
        readerConfig.setRsList(oceanBaseConf.getRootServerList());
        readerConfig.setUsername(oceanBaseConf.getUsername());
        readerConfig.setPassword(oceanBaseConf.getPassword());
        readerConfig.setTableWhiteList(
                String.format(
                        "%s.%s.%s",
                        oceanBaseConf.getTenantName(),
                        oceanBaseConf.getDatabaseName(),
                        oceanBaseConf.getTables()));
        if (lastSyncTimestamp > 0) {
            readerConfig.setStartTimestamp(lastSyncTimestamp);
            LOG.info("Read change events from lastSyncTimestamp:{}", lastSyncTimestamp);
        } else {
            readerConfig.setStartTimestamp(oceanBaseConf.getTimestamp());
            LOG.info("Read change events from startTimestamp:{}", oceanBaseConf.getTimestamp());
        }
        logProxyClient =
                new LogProxyClient(
                        oceanBaseConf.getLogProxyHost(),
                        oceanBaseConf.getLogProxyPort(),
                        readerConfig);
        recordListener =
                new OceanBaseRecordListener(lastSyncTimestamp, rowConverter, logProxyClient);
        logProxyClient.addListener(recordListener);
        logProxyClient.start();
        LOG.info("LogProxyClient started");
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        return recordListener.getData();
    }

    @Override
    protected void closeInternal() throws IOException {
        logProxyClient.stop();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }
}
