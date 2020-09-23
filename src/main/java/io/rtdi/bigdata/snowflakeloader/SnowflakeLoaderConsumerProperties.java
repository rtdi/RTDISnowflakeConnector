package io.rtdi.bigdata.snowflakeloader;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class SnowflakeLoaderConsumerProperties extends ConsumerProperties {

	public SnowflakeLoaderConsumerProperties(String name) throws PropertiesException {
		super(name);
	}

	public SnowflakeLoaderConsumerProperties(String name, TopicName topic) throws PropertiesException {
		super(name, topic);
	}

	public SnowflakeLoaderConsumerProperties(String name, String pattern) throws PropertiesException {
		super(name, pattern);
	}

}
