package com.dtstack.flinkx.connector.oceanbase.listener;

import com.dtstack.flinkx.converter.AbstractCDCRowConverter;

import org.apache.flink.table.data.RowData;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class OceanBaseRecordListener implements RecordListener {
    private static final Logger logger = LoggerFactory.getLogger(OceanBaseRecordListener.class);
    private Long lastSyncTimestamp;
    private final BlockingDeque<RowData> queue;
    private AtomicBoolean running = new AtomicBoolean(false);
    private final AbstractCDCRowConverter<LogMessage, RowData> rawConverter;
    private final LogProxyClient logProxyClient;

    public OceanBaseRecordListener(
            Long lastSyncTimestamp,
            AbstractCDCRowConverter<LogMessage, RowData> rawConverter,
            LogProxyClient logProxyClient) {
        this.lastSyncTimestamp = lastSyncTimestamp;
        this.rawConverter = rawConverter;
        this.logProxyClient = logProxyClient;
        queue = new LinkedBlockingDeque<>();
    }

    @Override
    public void notify(LogMessage logMessage) {
        switch (logMessage.getOpt()) {
            case HEARTBEAT:
            case BEGIN:
                running.compareAndSet(false, true);
                break;
            case INSERT:
            case UPDATE:
            case DELETE:
                if (!running.get()) {
                    break;
                }
                try {
                    queue.add(parseLogMessage(logMessage));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case COMMIT:
                lastSyncTimestamp = Long.parseLong(logMessage.getTimestamp());
                break;
            case DDL:
                logger.trace("Read DDL events :{}", logMessage.getFieldList().get(0).toString());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation: " + logMessage.getOpt());
        }
    }

    private RowData parseLogMessage(LogMessage logMessage) throws Exception {
        return rawConverter.toInternal(logMessage).poll();
    }

    @Override
    public void onException(LogProxyClientException e) {
        if (e.needStop()) {
            logProxyClient.stop();
        }
    }

    public RowData getData() {
        return queue.poll();
    }
}
