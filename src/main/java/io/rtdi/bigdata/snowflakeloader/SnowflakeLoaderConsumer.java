package io.rtdi.bigdata.snowflakeloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.snowflakeloader.table.SnowflakeRootTable;

public class SnowflakeLoaderConsumer extends Consumer<SnowflakeLoaderConnectionProperties, SnowflakeLoaderConsumerProperties> {
	Connection conn = null;
	private Map<Integer, SnowflakeRootTable> schemawriters = new HashMap<>();

	public SnowflakeLoaderConsumer(ConsumerInstanceController instance) throws IOException {
		super(instance);
		conn = SnowflakeLoaderFactory.getDatabaseConnection((SnowflakeLoaderConnectionProperties) this.getConnectionProperties());
	}

	@Override
	public void process(TopicName topic, long offset, long offsettimestamp, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException {
		RowType rowtype = ValueSchema.getChangeType(valueRecord);
		if (rowtype == null) {
			rowtype = RowType.UPSERT;
		}
		SnowflakeRootTable writer = getWriter(valueRecord);
		writer.writeRecord(valueRecord, rowtype, conn);
	}

	@Override
	protected void closeImpl() {
		for (SnowflakeRootTable w : schemawriters.values()) {
			w.close();
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Cannot close the Hana connection", e);
			}
		}
	}

	@Override
	public void fetchBatchStart() throws IOException {
	}

	@Override
	public void fetchBatchEnd() throws IOException {
	}

	@Override
	public void flushDataImpl() throws IOException {
		for (SnowflakeRootTable w : schemawriters.values()) {
			w.executeBatch();
		}
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to commit the data in Hana", e, null, null);
		}
	}
	
	private SnowflakeRootTable getWriter(JexlRecord record) throws ConnectorCallerException {
		SnowflakeRootTable w = schemawriters.get(record.getSchemaId());
		if (w == null) {
			w = new SnowflakeRootTable(this, record.getSchema());
			schemawriters.put(record.getSchemaId(), w);
		}
		return w;
	}

	public Connection getDBConnection() {
		return conn;
	}

}
