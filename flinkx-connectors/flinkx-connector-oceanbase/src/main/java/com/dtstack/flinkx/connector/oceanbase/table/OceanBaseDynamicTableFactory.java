package com.dtstack.flinkx.connector.oceanbase.table;

import com.dtstack.flinkx.connector.oceanbase.conf.OceanBaseConf;
import com.dtstack.flinkx.connector.oceanbase.options.Mode;
import com.dtstack.flinkx.connector.oceanbase.options.OceanBaseOptions;
import com.dtstack.flinkx.connector.oceanbase.source.OceanBaseDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Set;

import static com.dtstack.flinkx.connector.oceanbase.options.OceanBaseOptions.*;

public class OceanBaseDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "oceanbase";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        OceanBaseConf oceanBaseConf = new OceanBaseConf();
        oceanBaseConf
                .setHostname(config.get(HOSTNAME))
                .setPort(config.get(PORT))
                .setUsername(config.get(USERNAME))
                .setPassword(config.get(PASSWORD))
                .setTenantName(config.get(TENANT_NAME))
                .setDatabaseName(config.get(DATABASE_NAME))
                .setRootServerList(config.get(ROOT_SERVER_LIST))
                .setLogProxyHost(config.get(LOG_PROXY_HOST))
                .setLogProxyPort(config.get(LOG_PROXY_PORT))
                .setTables(config.get(TABLE_NAME))
                .setMode(Mode.valueOf(config.get(MODE).toUpperCase()))
                .setTimestamp(config.get(TIMESTAMP));

        return new OceanBaseDynamicTableSource(physicalSchema, oceanBaseConf);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return OceanBaseOptions.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return OceanBaseOptions.optionalOptions();
    }
}
