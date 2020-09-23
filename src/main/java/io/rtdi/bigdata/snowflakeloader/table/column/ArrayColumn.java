package io.rtdi.bigdata.snowflakeloader.table.column;

import java.util.List;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class ArrayColumn extends Column {

	private String schemaname;

	public ArrayColumn(Field f, List<String> path) {
		super(f, path);
		this.schemaname = IOUtils.getBaseSchema(f.schema()).getElementType().getFullName();
	}

	public String getSchemaName() {
		return schemaname;
	}

}
