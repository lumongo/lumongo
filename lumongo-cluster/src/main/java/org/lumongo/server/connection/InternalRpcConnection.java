package org.lumongo.server.connection;

import io.netty.bootstrap.Bootstrap;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.lumongo.cluster.message.Lumongo.InternalService;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.RpcClient;

public class InternalRpcConnection {
	
	private InternalService.BlockingInterface service;
	private RpcClient rpcClient;
	private Bootstrap bootstrap;
	private RpcController rpcController;
	
	public InternalRpcConnection(InternalService.BlockingInterface service, RpcClient rpcClient, Bootstrap bootstrap) {
		this.service = service;
		this.rpcClient = rpcClient;
		this.bootstrap = bootstrap;
		this.rpcController = null;
	}
	
	public InternalService.BlockingInterface getService() {
		return service;
	}
	
	public RpcController getClientRPCController() throws IOException {
		if (rpcClient != null) {
			rpcController = rpcClient.newRpcController();
			return rpcController;
		}
		throw new IOException("Connection is not open");
	}
	
	public Bootstrap getBootstrap() {
		return bootstrap;
	}
	
	public void close() {
		try {
			rpcController.startCancel();
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		try {
			if (rpcClient != null) {
				System.out.println("Closing connection to " + rpcClient.getPeerInfo());
				rpcClient.close();
			}
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		rpcClient = null;
		try {
			if (bootstrap != null) {
				bootstrap.group().shutdownGracefully(0, 15, TimeUnit.SECONDS);
			}
		}
		catch (Exception e) {
			System.err.println("Exception: " + e);
			e.printStackTrace();
		}
		bootstrap = null;
		service = null;
	}
}
