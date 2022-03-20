package com.dtstack.flinkx.connector.oceanbase.inputformat;

import com.dtstack.flinkx.connector.oceanbase.conf.OceanBaseConf;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

public class OceanBaseInputFormatBuilder extends BaseRichInputFormatBuilder {
    private final OceanBaseInputFormat format;

    public OceanBaseInputFormatBuilder() {
        super.format = format = new OceanBaseInputFormat();
    }

    public void setOceanBaseConfig(OceanBaseConf obConfig) {
        super.setConfig(obConfig);
        format.oceanBaseConf = obConfig;
    }

    public void setRawConverter(AbstractCDCRowConverter rawConverter) {
        this.format.setRowConverter(rawConverter);
    }

    @Override
    protected void checkFormat() {}
}
