package io.rtdi.bigdata.snowflakeloader.table.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.snowflakeloader.table.SnowflakeRootTable;
import io.rtdi.bigdata.snowflakeloader.table.SnowflakeTable;
import io.rtdi.bigdata.snowflakeloader.table.column.ArrayColumn;
import io.rtdi.bigdata.snowflakeloader.table.column.Column;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnPK;

public class SnowflakeWriterUpsert extends SnowflakeRootTableStatement {
	private Map<String, StatementData> childtableinsert;
	private Map<String, StatementData> childtabledelete;
	private StatementData insert;
	private StatementData delete;

	public SnowflakeWriterUpsert(SnowflakeRootTable writer, Connection conn) throws ConnectorCallerException {
		super(writer);
		StringBuffer sqltext = new StringBuffer();
		StringBuffer sqlparamlist = new StringBuffer();
		StringBuffer projection = new StringBuffer();
		
		try {
			sqltext.append("insert into ");
			sqltext.append(getWriter().getTableIdentifier());
			sqltext.append(" (");
			for (Column col : writer.getColumns()) {
				if (projection.length() != 0) {
					projection.append(", ");
					sqlparamlist.append(", ");
				}
				projection.append("\"").append(col.getColumnName()).append("\"");
				sqlparamlist.append("?");
			}
			sqltext.append(projection);
			sqltext.append(") values (");
			sqltext.append(sqlparamlist);
			sqltext.append(")");
			insert = new StatementData(conn.prepareStatement(sqltext.toString()));
			
			/* sqltext = new StringBuffer();
			projection = new StringBuffer();
			sqltext.append("delete from ");
			sqltext.append(getWriter().getTableIdentifier());
			sqltext.append(" where ");
			for (ColumnPK pk : getWriter().getPrimaryKeys()) {
				if (projection.length() != 0) {
					projection.append(" AND ");
				}
				projection.append("\"").append(pk.getColumnName()).append("\" = ?");
			}
			sqltext.append(projection);
			delete = conn.prepareStatement(sqltext.toString()); */
	
			createChildSQLs(conn);
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot create the insert/delete statement",
					e,
					"Execute the SQL manually to validate",
					sqltext.toString());
		}
	}
	
	@Override
	public void execute(JexlRecord record, RowType rowtype) throws ConnectorCallerException {
		if (record != null) {
			// deletechilddata(record);
			// deletedata(record);
			if (rowtype != RowType.DELETE && rowtype != RowType.EXTERMINATE) {
				insertdata(record);
				insertchilddata(record, writer);
			}
		}
	}
	
	private void insertchilddata(JexlRecord record, SnowflakeTable recordwriter) throws ConnectorCallerException {
		if (recordwriter.getArrayFields() != null) {
			for (Field field : recordwriter.getArrayFields().keySet()) {
				SnowflakeTable fieldwriter = recordwriter.getChildTable(field);
				ArrayColumn arraycolumn = recordwriter.getArrayFields().get(field);
				String schemaname = arraycolumn.getSchemaName();
				StatementData stmt = childtableinsert.get(schemaname);
				@SuppressWarnings("unchecked")
				List<JexlRecord> l = (List<JexlRecord>) arraycolumn.getValue(record);
				if (l != null && l.size() != 0) {
					try {
						for (JexlRecord r : l) {
							int pos = 1;
							for (Column col : fieldwriter.getColumns()) {
								Object value = col.getValue(r);
								stmt.setObject(pos, value);
								pos++;
							}
							stmt.addBatch();
							insertchilddata(r, fieldwriter);
						}
					} catch (SQLException e) {
						throw new ConnectorCallerException(
								"Cannot execute the insert statement into a child table",
								e,
								"Execute the SQL manually to validate",
								null);
					}
				}
			}
		}
	}

	private void insertdata(JexlRecord record) throws ConnectorCallerException {
		try {
			int pos = 1;
			for (Column col : getWriter().getColumns()) {
				Object value = col.getValue(record);
				insert.setObject(pos, value);
				pos++;
			}
			insert.addBatch();
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot execute the insert statement",
					e,
					"Execute the SQL manually to validate",
					null);
		}
	}

	private void deletedata(JexlRecord record) throws ConnectorCallerException {
		try {
			int pos = 1;
			for (Column col : getWriter().getPrimaryKeys()) {
				Object value = col.getValue(record);
				delete.setObject(pos, value);
				pos++;
			}
			delete.addBatch();
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot execute the delete statement",
					e,
					"Execute the SQL manually to validate",
					null);
		}
	}

	private void deletechilddata(JexlRecord record) throws ConnectorCallerException {
		for (StatementData stmt : childtabledelete.values()) {
			try {
				int pos = 1;
				for (Column col : writer.getPrimaryKeys()) {
					Object value = col.getValue(record);
					stmt.setObject(pos, value);
					pos++;
				}
				stmt.addBatch();
			} catch (SQLException e) {
				throw new ConnectorCallerException(
						"Cannot execute the delete statement",
						e,
						"Execute the SQL manually to validate",
						null);
			}
		}
	}

	@Override
	protected RowType getRowType() {
		return RowType.UPSERT;
	}

	private void createChildSQLs(Connection conn) throws SQLException {
		childtableinsert = new HashMap<>();
		childtabledelete = new HashMap<>();
		for (String name : writer.getAllChildTables().keySet()) {
			SnowflakeTable w = writer.getAllChildTables().get(name);
			StringBuffer sqltext = new StringBuffer();
			StringBuffer sqlparamlist = new StringBuffer();
			StringBuffer projection = new StringBuffer();
			sqltext.append("insert into ");
			sqltext.append(w.getTableIdentifier());
			sqltext.append(" (");
			for (Column col : w.getColumns()) {
				if (projection.length() != 0) {
					projection.append(", ");
					sqlparamlist.append(", ");
				}
				projection.append("\"").append(col.getColumnName()).append("\"");
				sqlparamlist.append("?");
			}
			sqltext.append(projection);
			sqltext.append(") values (");
			sqltext.append(sqlparamlist);
			sqltext.append(")");
			childtableinsert.put(name, new StatementData(conn.prepareStatement(sqltext.toString())));
			
			/* sqltext = new StringBuffer();
			projection = new StringBuffer();
			sqltext.append("delete from ");
			sqltext.append(w.getTableIdentifier());
			sqltext.append(" where ");
			for (ColumnPK pk : w.getPrimaryKeys()) {
				if (projection.length() != 0) {
					projection.append(" AND ");
				}
				projection.append("\"").append(pk.getColumnName()).append("\" = ?");
			}
			sqltext.append(projection);
			childtabledelete.put(name, conn.prepareStatement(sqltext.toString())); */
		}
	}
	
	@Override
	public String toString() {
		return "delete/insert for " + this.getWriter().getTableIdentifier();
	}

	@Override
	public void close() {
		closeStatement(delete);
		closeStatement(insert);
		if (childtabledelete != null) {
			for (StatementData stmt : childtabledelete.values()) {
				closeStatement(stmt);
			}
		}
		if (childtableinsert != null) {
			for (StatementData stmt : childtableinsert.values()) {
				closeStatement(stmt);
			}
		}
	}
	
	private void closeStatement(StatementData stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
		}
	}

	@Override
	public void executeBatch() throws ConnectorCallerException {
		executBatch(delete);
		executBatch(insert);
		if (childtabledelete != null) {
			for (StatementData stmt : childtabledelete.values()) {
				executBatch(stmt);
			}
		}
		if (childtableinsert != null) {
			for (StatementData stmt : childtableinsert.values()) {
				executBatch(stmt);
			}
		}
	}

	private void executBatch(StatementData stmt) throws ConnectorCallerException {
		if (stmt != null) {
			try {
				stmt.executeBatch();
			} catch (SQLException e) {
				throw new ConnectorCallerException(
						"Cannot execute the batch",
						e,
						"Execute the SQL manually to validate",
						null);
			}
		}
	}
	
	public static class StatementData {
		PreparedStatement stmt;
		int batchcount = 0;

		public StatementData(PreparedStatement stmt) {
			this.stmt = stmt;
		}

		public void close() throws SQLException {
			stmt.close();
		}

		public void executeBatch() throws SQLException {
			if (batchcount > 0) {
				stmt.executeBatch();
				batchcount = 0;
			}
		}

		public void addBatch() throws SQLException {
			stmt.addBatch();
			batchcount++;
		}

		public void setObject(int pos, Object value) throws SQLException {
			stmt.setObject(pos, value);
		}

	}

}