package io.rtdi.bigdata.snowflakeloader.table;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.snowflakeloader.SnowflakeLoaderConsumer;
import io.rtdi.bigdata.snowflakeloader.table.column.ArrayColumn;
import io.rtdi.bigdata.snowflakeloader.table.column.Column;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnFIELD;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnPARENTPATH;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnPATH;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnPK;
import io.rtdi.bigdata.snowflakeloader.table.column.ColumnRootPK;

/**
 * The HanaWriter is the holder of the target table information and creates/alters them.
 *
 */
public class SnowflakeTable {
	private SnowflakeLoaderConsumer consumer;
	private String tablename;
	private String tableidentifier;
	private SnowflakeRootTable root;
	private Schema schema;
	private List<Column> columns = new ArrayList<>();
	private Map<String, Integer> columnindex = new HashMap<>();
	private Map<Field, ArrayColumn> arrayfields;
	private Map<Field, SnowflakeTable> childtables;
	/**
	 * For the root table this is the exact primary key based on HanaColumnPK elements,
	 * for child tables it is only the list of column to be used in the delete statement.
	 */
	private List<ColumnPK> primarykeys = new ArrayList<>();


	public SnowflakeTable(SnowflakeLoaderConsumer consumer, Schema schema, SnowflakeRootTable root) throws ConnectorCallerException {
		this.consumer = consumer;
		if (root != null) {
			tablename = root.getTablename() + "_" + schema.getFullName();
		} else {
			tablename = schema.getFullName();
		}
		tableidentifier = "\"" + tablename + "\"";
		this.root = root;
		this.schema = schema;
		addColumns(schema, null);

		StringBuffer sql = new StringBuffer();
		sql.append("select column_name from information_schema.columns where table_schema = CURRENT_SCHEMA() and table_name = ? order by ordinal_position");
		try (PreparedStatement stmt = this.consumer.getDBConnection().prepareStatement(sql.toString()); ) {
			stmt.setString(1, tablename);
			try (ResultSet rs = stmt.executeQuery();) {
				if (rs.next()) {
					alterTable(rs);
				} else {
					createTable();
				}
			}
		} catch (SQLException e) {
			throw new ConnectorCallerException("Cannot read the table columns from the data dictionary information_schema.columns", e, "Execute the SQL manually to validate", sql.toString());
		}

		/*
		 * After building the entire (root) table, build the child tables
		 */
		if (arrayfields != null) {
			for (Field f : arrayfields.keySet()) {
				Schema s = IOUtils.getBaseSchema(f.schema());
				String schemaname = s.getElementType().getFullName();
				SnowflakeRootTable rw = getRootTable();
				SnowflakeTable w = rw.getChildTableFromDictionary(schemaname);
				if (w == null) {
					w = new SnowflakeTable(consumer, s.getElementType(), getRootTable());
					getRootTable().addChildTableToDictionary(schemaname, w);
				}
				addChildWriter(f, w);
			}
		}
	}
	
	public List<Column> getColumns() {
		return columns;
	}
	
	private void addColumn(Column column) {
		columns.add(column);
		columnindex.put(column.getColumnName(), columns.size()-1);
	}
	
	public List<ColumnPK> getPrimaryKeys() {
		return primarykeys;
	}
	
	private void addColumns(Schema schema, List<String> parentaccessor) throws ConnectorCallerException {
		/*
		 * All child tables have the PK columns of the root and some extra columns
		 */
		if (!(this instanceof SnowflakeRootTable)) {
			for (ColumnPK pk : getRootTable().getPrimaryKeys()) {
				ColumnRootPK rootPK = new ColumnRootPK(pk.getField());
				addColumn(rootPK);
				primarykeys.add(rootPK);
			}
			addColumn(new ColumnPATH());
			addColumn(new ColumnFIELD());
			addColumn(new ColumnPARENTPATH());
		}

		for (Field f : schema.getFields()) {
			Schema s = IOUtils.getBaseSchema(f.schema());
			if (s.getType() == Type.ARRAY) {
				addArrayField(new ArrayColumn(f, parentaccessor));
			} else if (s.getType() == Type.RECORD) {
				List<String> path = new ArrayList<>();
				if (parentaccessor != null) {
					path.addAll(parentaccessor);
				}
				path.add(AvroField.getOriginalName(f));
				addColumns(s, path);
			} else {
				if (AvroField.isPrimaryKey(f) && !AvroField.isInternal(f)) {
					ColumnPK pkcol = new ColumnPK(f);
					addColumn(pkcol);
					if (this instanceof SnowflakeRootTable) {
						primarykeys.add(pkcol);
					}
				} else {
					addColumn(new Column(f, parentaccessor));
				}
			}
		}
	}
	
	public Map<Field, SnowflakeTable> getChildTables() {
		return childtables;
	}
	
	public SnowflakeTable getChildTable(Field f) {
		return childtables.get(f);
	}
	
	public void addChildWriter(Field f, SnowflakeTable w) {
		if (childtables == null) {
			childtables = new HashMap<>();
		}
		childtables.put(f,w);
	}
	
	private void addArrayField(ArrayColumn hanaColumn) {
		if (arrayfields == null) {
			arrayfields = new HashMap<>();
		}
		arrayfields.put(hanaColumn.getField(), hanaColumn);
	}
	
	public Map<Field, ArrayColumn> getArrayFields() {
		return arrayfields;
	}

	protected String getTablename() {
		return tablename;
	}
	
	protected SnowflakeRootTable getRootTable() {
		return root;
	}

	private void alterTable(ResultSet rs) throws SQLException, ConnectorCallerException {
		// Create a set of all columns and remove those the table has columns for already
		Map<String, Integer> schemacolumns = new HashMap<>();
		schemacolumns.putAll(columnindex);
		do {
			String columnname = rs.getString(1);
			if (schemacolumns.containsKey(columnname)) {
				// column exists already
				schemacolumns.remove(columnname);
				// TODO: Handle data type changes and the such
			}
		} while (rs.next());
		StringBuffer sql = new StringBuffer();
		for (String columnname : schemacolumns.keySet()) {
			Column col = columns.get(schemacolumns.get(columnname));
			if (sql.length() != 0) {
				sql.append(", ");
			}
			sql.append("\"").append(columnname).append("\" ");
			sql.append(col.getDataTypeString());
		}
		if (sql.length() != 0) {
			StringBuffer preset = new StringBuffer();
			preset.append("alter table ").append(tableidentifier);
			preset.append(" add (");
			sql.insert(0, preset);
			sql.append(")");
			try (PreparedStatement stmt = this.consumer.getDBConnection().prepareStatement(sql.toString()); ) {
				stmt.execute();
			} catch (SQLException e) {
				throw new ConnectorCallerException("Failed to alter the table in Snowflake", e, "Execute the SQL manually", sql.toString());
			}
		}
	}

	private void createTable() throws ConnectorCallerException {
		StringBuffer sql = new StringBuffer();
		StringBuffer columnlist = new StringBuffer();
		StringBuffer pklist = new StringBuffer();
		sql.append("create table ").append(tableidentifier);
		sql.append(" (");

		for (Column col : columns) {
			if (columnlist.length() != 0) {
				columnlist.append(", ");
			}
			columnlist.append("\"").append(col.getColumnName()).append("\" ");
			columnlist.append(col.getDataTypeString());
			if (col instanceof ColumnPK) {
				if (pklist.length() != 0) {
					pklist.append(", ");
				}
				pklist.append("\"").append(col.getColumnName()).append("\"");
			}
		}
		sql.append(columnlist);
		
		if (pklist.length() != 0) {
			sql.append(", primary key (").append(pklist).append(")");
		}
		sql.append(")");
		try (PreparedStatement stmt = this.consumer.getDBConnection().prepareStatement(sql.toString()); ) {
			stmt.execute();
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to create the table in Snowflake", e, "Execute the SQL manually", sql.toString());
		}
	}

	public String getTableIdentifier() {
		return tableidentifier;
	}
	
	public SnowflakeLoaderConsumer getConsumer() {
		return consumer;
	}

	public Schema getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		return tablename;
	}
}