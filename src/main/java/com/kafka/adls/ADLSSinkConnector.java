package com.kafka.adls;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.log4j.Logger;

public class ADLSSinkConnector extends Connector{
	
	private Map<String, String> configProperties;
	final Logger logger = Logger.getLogger(ADLSSinkConnector.class);
	
	@Override
	public String version() {
		return "1.0";
	}

	@Override
	public void start(Map<String, String> props) {
		logger.debug("Entering ADLSSinkConnector::start");
		try {
		      configProperties = props;
		    } catch (ConfigException e) {
		      logger.error(e);
		      throw new ConnectException("Couldn't start Connector due to configuration error", e);
		}
		logger.debug("Exiting ADLSSinkConnector::start");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ADLSSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		logger.debug("Entering ADLSSinkConnector::taskConfigs");
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
		   taskConfigs.add(taskProps);
		}
		logger.debug("Exiting ADLSSinkConnector::taskConfigs");
		return taskConfigs;		    
	}

	@Override
	public void stop() {
		//Stop() method
	}

	@Override
	public ConfigDef config() {
		 return null;
	}

}
