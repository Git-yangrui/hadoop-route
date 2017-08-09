package com.yangrui.hadoop.RPC;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L,
				new InetSocketAddress("192.168.1.3", 10000), conf);
		proxy.login("mingzhu", "1234");
	}
}
