package org.lumongo.util;

import java.net.URL;

import org.apache.log4j.PropertyConfigurator;
import org.lumongo.server.LuceneNode;

public class LogUtil {
	
	private final static Object lock = new Object();
	
	private static boolean loaded = false;
	
	private LogUtil() {
		
	}
	
	public static void loadLogConfig() throws Exception {
		synchronized (lock) {
			
			if (!loaded) {
				String propPath = "/etc/loglevel.properties";
				URL url = LuceneNode.class.getResource(propPath);
				if (url == null) {
					throw new Exception("Cannot find log properties file: " + propPath);
				}
				PropertyConfigurator.configure(url);
				loaded = true;
			}
		}
		
	}
}
