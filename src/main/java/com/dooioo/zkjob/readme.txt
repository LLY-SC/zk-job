/**
1.配置文件：src/java/resources/global.properties
2.参数：
	env：test（production，integration）
	如： {env}_zookeeperUrl
	
	For Example：
	production_zookeeperUrl
	test_zookeeperUrl=10.8.1.121:2181||10.8.1.122:2181 --defualt value 10.8.1.121:2181||10.8.1.122:2181 （多个zk server节点请用“||”隔开）
	appCode=Training   default ""

3.注解@ZkJob使用

	参数列表：type  &&  dateDiff
	

	type：两个参数可选，1.ZkJobType.INTERVAL   2.ZkJobType. ONETIME

	dateDiff:间隔时间，单位是秒。
	
	example:

	如果是一天执行一次，或者是一周一个月执行一次。@ZkJob(type=ZkJobType.ONETIME)
	如果是间隔执行（间隔时间小于一天）。 如5分钟一次，@ZkJob(type=ZkJobType.INTERVAL,dateDiff=5*60)



4.附上参数类代码：
	
	@zkJob code
	//默认是一天
	ZkJobType type()  default ZkJobType.INTERVAL;
	long dateDiff() default 24*60*60;
	

	@ZkJobType  <code>
	 enum ZkJobType {
			/*
			 * 固定时间一次性的
			 */
			ONETIME,
			/*
			* 可以间隔重复执行的
			**/
			INTERVAL;
	}

