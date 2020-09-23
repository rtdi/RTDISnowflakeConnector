package io.rtdi.bigdata.snowflakeloader.table.writer;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.snowflakeloader.table.SnowflakeRootTable;

public abstract class SnowflakeRootTableStatement {

	protected SnowflakeRootTable writer;

	public SnowflakeRootTableStatement(SnowflakeRootTable writer) {
		this.writer = writer;
	}

	protected abstract RowType getRowType();

	public SnowflakeRootTable getWriter() {
		return writer;
	}

	public abstract void execute(JexlRecord record, RowType rowtype) throws ConnectorCallerException;
	
	public abstract void close();
	
	public abstract void executeBatch() throws ConnectorCallerException;

}
