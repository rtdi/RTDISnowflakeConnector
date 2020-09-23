package io.rtdi.bigdata.snowflakeloader.table.column;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.IAvroNested;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;

public class ColumnFIELD extends Column {

	public static final String NAME = "__FIELD";
	public ColumnFIELD() {
		super(NAME, "NVARCHAR(256)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		IAvroNested d = record.getParent();
		if (d != null && d.getParentField() != null) {
			return AvroField.getOriginalName(d.getParentField());
		} else {
			return null;
		}
	}
	
}
