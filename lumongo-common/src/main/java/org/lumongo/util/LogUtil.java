package org.lumongo.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LogUtil {
	
	private final static Object lock = new Object();
	
	private static boolean loaded = false;
	
	private LogUtil() {
		
	}
	
	public static void loadLogConfig() throws Exception {
		synchronized (lock) {
			
			if (!loaded) {
				ConsoleAppender console = new ConsoleAppender(); //create appender
				//configure the appender
				String PATTERN = "%d [%t] <%p> %c{2}: %m%n";
				console.setLayout(new PatternLayout(PATTERN));
				console.setThreshold(Level.INFO);
				console.activateOptions();
				//add appender to any Logger (here is root)
				Logger.getRootLogger().addAppender(console);
				
				//String propPath = "/etc/loglevel.properties";
				//URL url = LogUtil.class.getResource(propPath);
				//if (url == null) {
				//	throw new Exception("Cannot find log properties file: " + propPath);
				//}
				//PropertyConfigurator.configure(url);
				loaded = true;
			}
		}
		
	}
}
