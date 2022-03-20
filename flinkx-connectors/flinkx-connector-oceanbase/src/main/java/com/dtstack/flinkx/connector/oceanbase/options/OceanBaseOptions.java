package com.dtstack.flinkx.connector.oceanbase.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

public class OceanBaseOptions {
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Ip address or hostname of the OceanBase database server or OceanBase proxy server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(2881)
                    .withDescription(
                            "Port of  OceanBase database server or OceanBase proxy server.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Username to connect to OceanBase database."
                                    + "The format is username@tenant_name#cluster_name when connecting to OceanBase proxy server"
                                    + "or username@tenant_name when directly connecting to OceanBase server.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to connect to OceanBase database.");
    public static final ConfigOption<String> TENANT_NAME =
            ConfigOptions.key("tenant-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tenant name of OceanBase database to monitor.");
    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of OceanBase database to monitor.");
    public static final ConfigOption<String> ROOT_SERVER_LIST =
            ConfigOptions.key("rootserver-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Root server list of OceanBase cluster in format 'ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2'.");
    public static final ConfigOption<String> LOG_PROXY_HOST =
            ConfigOptions.key("logproxy-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or ip address of OceanBase log proxy service.");
    public static final ConfigOption<Integer> LOG_PROXY_PORT =
            ConfigOptions.key("logproxy-port")
                    .intType()
                    .defaultValue(2983)
                    .withDescription("Post of OceanBase log proxy service.");
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names to be monitored, separated by commas in format: table name 1,table name 2,table name 3...");
    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .defaultValue(Mode.LATEST.toString())
                    .withDescription(
                            "Optional mode for OceanBase CDC, "
                                    + "valid enumerations are \"initial\",\"latest\",\"timestamp\", default is LATEST.");
    public static final ConfigOption<Long> TIMESTAMP =
            ConfigOptions.key("timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional timestamp in seconds to use in \"timestamp\" mode.");

    public static Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TENANT_NAME);
        options.add(DATABASE_NAME);
        options.add(ROOT_SERVER_LIST);
        options.add(LOG_PROXY_HOST);
        options.add(TABLE_NAME);
        return options;
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(MODE);
        options.add(TIMESTAMP);
        options.add(PORT);
        options.add(LOG_PROXY_PORT);
        return options;
    }
}
