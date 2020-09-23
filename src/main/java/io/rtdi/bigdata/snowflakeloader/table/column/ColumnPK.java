package io.rtdi.bigdata.snowflakeloader.table.column;

import org.apache.avro.Schema.Field;

public class ColumnPK extends Column {

	public ColumnPK(Field f) {
		super(f, null);
	}

	public ColumnPK(String name, String hanadatatype) {
		super(name, hanadatatype);
	}

}
