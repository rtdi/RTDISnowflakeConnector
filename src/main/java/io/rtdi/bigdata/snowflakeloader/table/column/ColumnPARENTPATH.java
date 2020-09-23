package io.rtdi.bigdata.snowflakeloader.table.column;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ColumnPARENTPATH extends Column {

	public static final String NAME = "__PARENT_RECORD_PATH";

	public ColumnPARENTPATH() {
		super(NAME, "NVARCHAR(5000)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		return record.getParent().getPath();

	}
}
