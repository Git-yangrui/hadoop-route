package com.ibeifeng.sparkproject.conf;

import org.junit.Assert;
import org.junit.Test;

/**
 * 配置管理组件测试类
 * @author Administrator
 *
 */
public class ConfigurationManagerTest {
    @Test
	public void test_ConfigurationManager(){
		String testkey1 = ConfigurationManager.getProperty("testkey1");
		String testkey2 = ConfigurationManager.getProperty("testkey2");
		Assert.assertEquals(testkey1,"testKey1");
		Assert.assertEquals(testkey2,"testKey2");
	}
	
}
