package io.rtdi.bigdata.snowflakeloader.table.writer;

import java.sql.Connection;
import java.sql.SQLException;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.snowflakeloader.table.SnowflakeTable;

public abstract class SnowflakeWriterStatement {
	protected SnowflakeTable writer;
	
	public SnowflakeWriterStatement(SnowflakeTable writer) {
		super();
		this.writer = writer;
	}

	protected abstract void execute(Connection conn) throws SQLException, ConnectorCallerException;
	
	public SnowflakeTable getWriter() {
		return writer;
	}

}