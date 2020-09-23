package io.rtdi.bigdata.snowflakeloader.table;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.snowflakeloader.SnowflakeLoaderConsumer;
import io.rtdi.bigdata.snowflakeloader.table.writer.SnowflakeRootTableStatement;
import io.rtdi.bigdata.snowflakeloader.table.writer.SnowflakeWriterUpsert;

public class SnowflakeRootTable extends SnowflakeTable {
	private Map<String, SnowflakeTable> allchildtables;
	private SnowflakeRootTableStatement writer;


	public SnowflakeRootTable(SnowflakeLoaderConsumer consumer, Schema schema) throws ConnectorCallerException {
		super(consumer, schema, null);
	}
	
	@Override
	protected SnowflakeRootTable getRootTable() {
		return this;
	}

	public Map<String, SnowflakeTable> getAllChildTables() {
		return allchildtables;
	}
	
	public SnowflakeTable getChildTableFromDictionary(String schemaname) {
		if (allchildtables == null) {
			return null;
		} else {
			return allchildtables.get(schemaname);
		}
	}
	
	public void addChildTableToDictionary(String schemaname, SnowflakeTable hanaWriter) {
		if (allchildtables == null) {
			allchildtables = new HashMap<>();
		}
		allchildtables.put(schemaname, hanaWriter);
	}
	
	public void writeRecord(JexlRecord record, RowType rowtype, Connection conn) throws ConnectorCallerException {
		if (writer == null) {
			writer = new SnowflakeWriterUpsert(this, conn);
		}
		writer.execute(record, rowtype);
	}

	public void executeBatch() throws ConnectorCallerException {
		writer.executeBatch();
	}

	public void close() {
		writer.close();
	}

}