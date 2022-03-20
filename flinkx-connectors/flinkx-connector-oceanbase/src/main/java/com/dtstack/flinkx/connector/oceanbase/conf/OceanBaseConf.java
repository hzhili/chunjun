package com.dtstack.flinkx.connector.oceanbase.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.oceanbase.options.Mode;

import java.util.Objects;
import java.util.StringJoiner;

public class OceanBaseConf extends FlinkxCommonConf {
    private String hostname;
    private int port;
    private String username;
    private String password;
    private String tenantName;
    private String databaseName;
    private String rootServerList;
    private String logProxyHost;
    private int logProxyPort;
    private String tables;
    private Mode mode = Mode.LATEST;
    private Long timestamp;

    public String getHostname() {
        return hostname;
    }

    public OceanBaseConf setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int getPort() {
        return port;
    }

    public OceanBaseConf setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public OceanBaseConf setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public OceanBaseConf setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getRootServerList() {
        return rootServerList;
    }

    public OceanBaseConf setRootServerList(String rootServerList) {
        this.rootServerList = rootServerList;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public OceanBaseConf setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getTenantName() {
        return tenantName;
    }

    public OceanBaseConf setTenantName(String tenantName) {
        this.tenantName = tenantName;
        return this;
    }

    public String getTables() {
        return tables;
    }

    public OceanBaseConf setTables(String tables) {
        this.tables = tables;
        return this;
    }

    public Mode getMode() {
        return mode;
    }

    public OceanBaseConf setMode(Mode mode) {
        this.mode = mode;
        return this;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public OceanBaseConf setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public OceanBaseConf setLogProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
        return this;
    }

    public int getLogProxyPort() {
        return logProxyPort;
    }

    public OceanBaseConf setLogProxyPort(int logProxyPort) {
        this.logProxyPort = logProxyPort;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OceanBaseConf that = (OceanBaseConf) o;
        return Objects.equals(hostname, that.hostname)
                && Objects.equals(port, that.port)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(rootServerList, that.rootServerList)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tenantName, that.tenantName)
                && Objects.equals(tables, that.tables)
                && mode == that.mode
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(logProxyHost, that.logProxyHost)
                && Objects.equals(logProxyPort, that.logProxyPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                hostname,
                port,
                username,
                password,
                rootServerList,
                databaseName,
                tenantName,
                tables,
                mode,
                timestamp,
                logProxyHost,
                logProxyPort);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", OceanBaseConf.class.getSimpleName() + "[", "]")
                .add("hostname='" + hostname + "'")
                .add("port='" + port + "'")
                .add("username='" + username + "'")
                .add("password='" + password + "'")
                .add("rootServerList='" + rootServerList + "'")
                .add("databaseName='" + databaseName + "'")
                .add("tenantName='" + tenantName + "'")
                .add("tables='" + tables + "'")
                .add("mode=" + mode)
                .add("timestamp='" + timestamp + "'")
                .add("logProxyHost='" + logProxyHost + "'")
                .add("logProxyPort='" + logProxyPort + "'")
                .toString();
    }
}
