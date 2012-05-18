package org.lumongo.util.properties;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertiesReader {
	
	public static class PropertyException extends Exception {
		private static final long serialVersionUID = 1L;
		
		public PropertyException(String name, String key, String message) {
			super("Failed load key <" + key + "> in <" + name + ">. " + message);
		}
		
	}
	
	private final String name;
	
	private Properties properties;
	
	protected PropertiesReader(String name) {
		this.name = name;
	}
	
	public PropertiesReader(File file) throws FileNotFoundException, IOException {
		properties = new Properties();
		properties.load(new FileReader(file));
		name = file.getAbsolutePath();
	}
	
	public PropertiesReader(Class<?> askingClass, String configFilePath) throws IOException {
		name = askingClass + ":" + configFilePath;
		
		InputStream is = askingClass.getResourceAsStream(configFilePath);
		if (is == null) {
			throw new IOException("Config file <" + configFilePath + "> does not exist on classpath");
		}
		
		properties = new Properties();
		properties.load(new InputStreamReader(is));
	}
	
	public int getInteger(String key) throws PropertyException {
		String value = getString(key);
		try {
			return Integer.parseInt(value);
		}
		catch (NumberFormatException e) {
			throw new PropertyException(this.name, key, "Failed to parse key as integer.");
		}
	}
	
	public double getDouble(String key) throws PropertyException {
		String value = getString(key);
		try {
			return Double.parseDouble(value);
		}
		catch (NumberFormatException e) {
			throw new PropertyException(this.name, key, "Failed to parse key as double.");
		}
	}
	
	public boolean getBoolean(String key) throws PropertyException {
		String value = getString(key);
		
		value = value.toLowerCase();
		if (value.equals("true")) {
			return true;
		}
		else if (value.equals("false")) {
			return false;
		}
		
		throw new PropertyException(this.name, key, "Failed to parse key as boolean: expected true or false.");
	}
	
	public String[] getStringArray(String key) throws PropertyException {
		return getStringArray(key, ";");
	}
	
	public String[] getStringArray(String key, String delimiter) throws PropertyException {
		String value = getString(key);
		return value.split(delimiter);
	}
	
	public String getString(String key) throws PropertyException {
		String value = properties.getProperty(key);
		if (value != null) {
			return value;
		}
		throw new PropertyException(this.name, key, "Failed to find key.");
	}
	
}
