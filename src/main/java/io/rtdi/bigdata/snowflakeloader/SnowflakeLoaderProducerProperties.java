package io.rtdi.bigdata.snowflakeloader;

import java.io.File;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class SnowflakeLoaderProducerProperties extends ProducerProperties {

	public SnowflakeLoaderProducerProperties(String name) throws PropertiesException {
		super(name);
	}

	public SnowflakeLoaderProducerProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

}
