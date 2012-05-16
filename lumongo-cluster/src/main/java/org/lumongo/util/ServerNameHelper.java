package org.lumongo.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class ServerNameHelper {
	public static String getLocalServer() throws UnknownHostException, SocketException {
		Enumeration<NetworkInterface> nicList;
		NetworkInterface nic;
		Enumeration<InetAddress> nicAddrList;
		InetAddress nicAddr;
		
		nicList = NetworkInterface.getNetworkInterfaces();
		while (nicList.hasMoreElements()) {
			nic = nicList.nextElement();
			if (!nic.isLoopback() && nic.isUp()) {
				nicAddrList = nic.getInetAddresses();
				while (nicAddrList.hasMoreElements()) {
					nicAddr = nicAddrList.nextElement();
					if (nicAddr instanceof Inet4Address) {
						return nicAddr.getCanonicalHostName();
					}
				}
			}
		}
		
		return null;
		
	}
}
