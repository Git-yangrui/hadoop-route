package com.yangrui.hadoop.RPC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer {
	public static void main(String[] args) throws Exception, IOException {
		Configuration config = new Configuration();
		RPC.Builder builder = new RPC.Builder(config);
		builder.setInstance(new LoginServiceImpl())
		.setBindAddress("192.168.1.3").setPort(10000)
		.setProtocol(LoginServiceInterface.class);
		Server build = builder.build();
		build.start();
	}
}
