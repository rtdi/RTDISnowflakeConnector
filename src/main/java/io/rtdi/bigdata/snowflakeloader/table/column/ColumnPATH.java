package io.rtdi.bigdata.snowflakeloader.table.column;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ColumnPATH extends ColumnPK {

	public static final String NAME = "__RECORD_PATH";

	public ColumnPATH() {
		super(NAME, "NVARCHAR(5000)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		return record.getPath();
	}
}
