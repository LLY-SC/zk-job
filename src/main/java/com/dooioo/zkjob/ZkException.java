package com.dooioo.zkjob;

public class ZkException extends Exception{

	/**
	 * 自定义异常中断程序
	 */
	private static final long serialVersionUID = 1L;
	public ZkException(String mssg) {
		super(mssg);
	}
}
