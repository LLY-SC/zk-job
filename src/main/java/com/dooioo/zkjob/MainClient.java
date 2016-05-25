package com.dooioo.zkjob;

import java.util.Calendar;

import javax.annotation.PreDestroy;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.dooioo.plus.util.GlobalConfigUtil;
/**
 * <pre>
 * @introduction 思路：利用zk临时节点的先后顺序实现锁机制。
 * 根节点 持久节点：locks/testNode，各个工程将创建/locks/testNode/lock_1...../lock/testNode/lock_1n 临时子节点，
 * 各个应用将建立长连接到zkserver,并自动心跳检测链接有效性（互相询问）。
 * 
	 * 获取/locks/testNode/lock_1到达的顺序，如果是最先到达的就执行。实际是获取
	 * 排序后的lock_x，排序后取下第一个判断是否和各个应用获取的相等。相等即获得锁，其他的等待，并且watch testNode节点。由于
	 * 是多个应用，不再是单一应用所谓的多线程。仍然需要zk提供数据来决定如下异常或正常情况。
	 * 1.1如job_1一次性安全执行完毕，通知其他应用结束等待，并结束通信，或者跳过job。如何实现?
	 * job_1正常结束之后需要通知testNode  将data的数据由start---> end，其他的client均watch的data变化，如果等于end则自行了断。
	 * job_1不正常结束，如抛出异常，异常的应用捕获后删掉自己创建的node，那么
	 * children数会变少， 其余client  watch lock的children数量，少了就重新洗牌。
	 * 
	 * 那么，对lock的watch应该是  children数量（谁去执行），data数据（是否结束等待）。
	 * </pre>
 * @author liuyang
 *@setting 需要在global.propertis文件中设置两个参数
 */
@Component
@Scope(value="singleton")
public class MainClient {
	PropertiesConfiguration prop = new PropertiesConfiguration();
	protected static String zookeeperUrl = "10.8.1.121:2181,10.8.1.122:2181";
	protected static String root = "/locks/";
	protected static int connectionTimeout = 30000;
	public static ZkClient zk =null;
	protected static Integer mutex=1;
	protected static String env;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	//因为机器时间不一致，所以给出时间差的五秒误差
	protected static long errorSize=5;//单位：秒
	//取得时间,用于每天只能执行一次的job判断（当然隔一月，一周执行一次和隔一天执行一次等同对待）
	String date ="";
	public MainClient() {
	//首次链接直接建立客户端
		synchronized(mutex){
		if (zk == null) {
			prop.setEncoding("utf-8");
			try {
				prop.load("global.properties");
				env = GlobalConfigUtil.getCurrentEnv();//prop.getString("env");
				if(root.equals("/locks/"))
				root=root+prop.getString("appCode", "");
				zookeeperUrl=prop.getString(env+"_"+"zookeeperUrl", zookeeperUrl).replace("||", ",");
				
				//注册zk中心的服务
				zk = new ZkClient(zookeeperUrl, connectionTimeout);
				logger.info("zk has bean conn,root is "+root);
				if (!zk.exists(root)) {
					try{
					zk.createPersistent(root,true);
					logger.info("root node has been created: "+root);
					}catch(ZkNodeExistsException zke){
						zke.printStackTrace();
						org.slf4j.LoggerFactory.getLogger(this.getClass()).info("root node exist ,自动略过");
					}
					
				}
			} catch (Exception e) {
				zk = null;
				e.printStackTrace();
			}
		}
	}
	}
	//如果会话超时等原因链接被服务器拒绝，此时zk=closed检测状态重新链接
	public void retryConn(){
		Calendar c = Calendar.getInstance();
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH)+1;
		int day = c.get(Calendar.DATE);
		date =""+year+month+day;
		if(zk==null||zk.getShutdownTrigger()==true){
			zk = new ZkClient(zookeeperUrl, connectionTimeout);
		}
	}

	@PreDestroy
	public void destory(){
		if (zk.exists(root)) {
			zk.delete(root);
			zk.close();
		}
	}
	
	}

