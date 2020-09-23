package io.rtdi.bigdata.snowflakeloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryConsumer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class SnowflakeLoaderFactory extends ConnectorFactory<SnowflakeLoaderConnectionProperties>
implements IConnectorFactoryConsumer<SnowflakeLoaderConnectionProperties, SnowflakeLoaderConsumerProperties>{

	public SnowflakeLoaderFactory() {
		super("SnowflakeLoader");
	}

	@Override
	public Consumer<SnowflakeLoaderConnectionProperties, SnowflakeLoaderConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
		return new SnowflakeLoaderConsumer(instance);
	}

	@Override
	public SnowflakeLoaderConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new SnowflakeLoaderConnectionProperties(name);
	}

	@Override
	public SnowflakeLoaderConsumerProperties createConsumerProperties(String name) throws PropertiesException {
		return new SnowflakeLoaderConsumerProperties(name);
	}

	@Override
	public BrowsingService<SnowflakeLoaderConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new SnowflakeLoaderBrowse(controller);
	}

	@Override
	public boolean supportsBrowsing() {
		return false;
	}

	static Connection getDatabaseConnection(SnowflakeLoaderConnectionProperties props) throws ConnectorCallerException {
		try {
			return getDatabaseConnection(props.getJDBCURL(), props.getUsername(), props.getPassword());
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to establish a database connection", e, null, props.getJDBCURL());
		}
	}
	
	static Connection getDatabaseConnection(String jdbcurl, String user, String passwd) throws SQLException {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            Connection conn = DriverManager.getConnection(jdbcurl, user, passwd);
            conn.setAutoCommit(false);
            return conn;
        } catch (ClassNotFoundException e) {
            throw new SQLException("No Hana JDBC driver library found");
        }
	}

}
