package com.dtstack.flinkx.connector.oceanbase.options;

public enum Mode {
    /** 首次启动时进行初始化快照同步,并继续读取日志 */
    INITIAL,
    /** 从日志的末尾开始读取 */
    LATEST,
    /** 从指定的时间戳读取日志 */
    TIMESTAMP;
}
