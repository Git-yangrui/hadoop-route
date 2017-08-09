package com.yangrui.hadoop.RPC;

public class LoginServiceImpl implements LoginServiceInterface{

	@Override
	public String login(String username, String password) {
		System.out.println("-----------------");
		return username+"login in successfully";
	}

}
