package io.rtdi.bigdata.snowflakeloader;

import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class SnowflakeLoaderConnectionProperties extends ConnectionProperties {

	private static final String JDBCURL = "snowflake.jdbcurl";
	private static final String USERNAME = "snowflake.username";
	private static final String PASSWORD = "snowflake.password";

	public SnowflakeLoaderConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(JDBCURL, "JDBC URL", "The JDBC URL to use for connecting to the Snowflake system", "sap-icon://target-group", "jdbc:snowflake://<account_name>.snowflakecomputing.com/?db=<name>&schema=<name>", true);
		properties.addStringProperty(USERNAME, "Username", "Snowflake username", "sap-icon://target-group", null, true);
		properties.addPasswordProperty(PASSWORD, "Password", "Password", "sap-icon://target-group", null, true);
	}

	public String getJDBCURL() {
		return properties.getStringPropertyValue(JDBCURL);
	}
	
	public String getUsername() {
		return properties.getStringPropertyValue(USERNAME);
	}
	
	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}
	
}
