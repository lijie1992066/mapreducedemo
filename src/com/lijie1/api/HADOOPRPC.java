package com.lijie1.api;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

public class HADOOPRPC {
	public static void main(String[] args) throws Exception {
		
		//创建建造对象
		Builder b = new RPC.Builder(new Configuration());
		
		//绑定参数 
		//第一个为ip
		//第二个为端口
		//第三个为协议接口
		//第四个为业务实例
		b.setBindAddress("localhost").setPort(9876).setProtocol(MyInterface.class).setInstance(new MyImpl());
		
		//创建服务并启动
		b.build().start();
		
		//创建代理对象
		MyInterface myProxy = RPC.getProxy(MyInterface.class, 10086L, new InetSocketAddress("localhost", 9876), new Configuration());
	
		//远程调用服务器方法
		String str = myProxy.getStr();
		
		//打印
		System.out.println(str);
	}
}
