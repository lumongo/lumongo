package org.lumongo.server.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ConnectionHelper {
	private final static String myHostName;
	static {
		
		try {
			myHostName = InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public static String getHostName() {
		return myHostName;
	}
}
