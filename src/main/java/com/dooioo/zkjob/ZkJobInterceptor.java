package com.dooioo.zkjob;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

/**
 * 切面功能描述：配合注解@ZkJob. ，利用zookeeper分布式锁，完成分布式spring Job的调度。 zookeeper 目录结构:
 * 
 * <pre>
 * 			                	   /locks 
 * 						|	 |	 |
 * 			              __appCode1__   appCode2   appCode3    ……    （persistent Znode）
 * 				          |                |							
 * 		           __classFullName1__   classFullName2   ……    （persistent Znode）
 *                               |	  |	| 
 *             			lock_1	lock_2	lock_3   ……     （EphemeralSequential Znode）
 * </pre>
 * 
 * @scope 使用prototype 预防可能同一时间有多个job均在执行，单例会给多线程造成困扰。
 * @author liuyang
 * @version 1.0
 *
 */
@Aspect
@Component
@Scope(value=ConfigurableBeanFactory.SCOPE_PROTOTYPE,proxyMode=ScopedProxyMode.DEFAULT)
public class ZkJobInterceptor extends MainClient implements
		ApplicationContextAware {
	Logger logger = LoggerFactory.getLogger(this.getClass());

	// 继承于org.springframework.context.ApplicationContextAware spring上下文
	private static ApplicationContext context = null;

	// 每个job建立一个persistent 根节点，全路径为root/job类名
	private String thisLock = "";

	// 间隔时间
	private long dateDiff;

	// 本应用建立的EphemeralSequential node,全路径为root/job类名/lock_
	private String myZnode;

	// 保存每次获取锁的node路径
	private String getLockNode;

	// 注解参数数组
	private ZkJobType zkjt;

	@SuppressWarnings("rawtypes")
	// 反射使用的目标类
	private Class targetClass_;
	// 反射使用的目标方法
	private Method targetMethod;

	// 设置切入点
	@Pointcut("@annotation(com.dooioo.zkjob.ZkJob)")
	public void jobAspect() {

	}

	/**
	 * 前置通知，建立zk子节点
	 * 
	 * @param joinpoint
	 *            切点
	 * @throws Exception
	 */
	@Before("jobAspect()")
	public void dobefore(JoinPoint joinpoint) throws Exception {
		logger.info("before doing");
		retryConn();
		Object[] description = getZkJobAnnotationDescription(joinpoint);
		// thisLock = root + "/testNode";
		System.out.println(thisLock);
		dateDiff = (long) description[0] * 1000;
		if (zk != null) {
			zkjt = (ZkJobType) description[1];
			// 第一次建立节点需要特殊判断
			boolean isfisrtTime = false;
			// 创建类名节点
			if (!zk.exists(thisLock)) {
				try {
					zk.createPersistent(thisLock, true);
					isfisrtTime = true;
				} catch (ZkNodeExistsException zke) {
					zke.printStackTrace();
					logger.info("node exists ....,自动略过");
				}
			}
			// 创建锁
			if (myZnode == null) {
				myZnode = zk.createEphemeralSequential(thisLock + "/lock_", "");
				logger.info(" EphemeralSequential node created: " + myZnode);
			}
			/*
			 * 1.如果是间隔执行的，就根据本次上次执行的时间差来判断是否需要执行，目的是防止
			 * 由于服务器时间不一致引起的短时间重复执行（虽然这是极端的情况） 存入thisLock的是时间间隔
			 */
			if (zkjt.equals(ZkJobType.INTERVAL)) {
				long thisUpdateTime = zk.getCreationTime(myZnode);
				String read = zk.readData(thisLock) == null ? "0" : zk
						.readData(thisLock);
				long lastUpdateTime = Long.valueOf((read).toString()
						.replace("end", "").replace("start", ""));
				// 对比本次,上次 执行的时间。机器时间没法做到严格一致，所以时间差暂时允许五秒的误差errorSize=5秒
				if (((thisUpdateTime - lastUpdateTime) / 1000 + errorSize) < dateDiff / 1000
						&& isfisrtTime == false
						&& read.toString().contains("end")) {
					logger.info("thisUpdateTime is " + thisUpdateTime + ","
							+ "lastUpdateTime is " + lastUpdateTime
							+ ",dateDiff is " + dateDiff);
					logger.info("(thisUpdateTime - lastUpdateTime)/1000 is"
							+ (thisUpdateTime - lastUpdateTime) / 1000
							+ ",dateDiff/1000 is " + dateDiff / 1000);
					logger.info("修正后的差值是"
							+ (long) ((thisUpdateTime - lastUpdateTime) / 1000 + errorSize));
					deleteMyZnode();
					logger.info("dateDiff less than :" + dateDiff / 1000
							+ "s,canceled this time ,wait for next time");
					removeWatch();
					throw new ZkException(
							"annotation @zkJob 前置通知抛出异常，job中断，原因是在一个时间间隔内有重复执行,时间间隔为："
									+ dateDiff / 1000 + " 秒");
				} else {
					zk.writeData(thisLock, thisUpdateTime + "start");
				}
			}
			/**
			 * 如果是一次性的，那么执行一次就不能再执行，存入的是年月日
			 */
			if (zkjt.equals(ZkJobType.ONETIME)) {
				String read = zk.readData(thisLock);
				if (read != null) {
					if (read.equals(date + "end")) {
						logger.info("job has been finished tody:" + date
								+ ",please do it next day");
						throw new ZkException(
								"annotation @zkJob 前置通知抛出异常，job中断，原因是在同一天内重复执行");
					} else
						zk.writeData(thisLock, date + "start");
				} else
					zk.writeData(thisLock, date + "start");
			}
		} else {
			// 发送短信吧亲，日志输出吧。链接失败了，job无法执行。
			logger.error("node: " + thisLock + "job:" + targetClass_
					+ "，失败了,error：zk链接失败");
			throw new ZkException("zk链接失败,请手动检查");
		}

	}

	/**
	 * 环绕通知：加入监听，判断程序是否执行
	 * 
	 * @param pjp
	 * @return
	 * @throws Throwable
	 */
	@Around(value = "jobAspect()")
	public Object aroundAdvice(ProceedingJoinPoint pjp) throws Throwable {
		setWatch();
		Object retVal = null;
		getLockNode = getNode0();
		//if(getLockNode ==null)
			//throw new ZkException("getLockNode is null ,job has not finished before all node deleted,pleas check");
			//return null;
		logger.info("i am " + myZnode + ",my getLockNode  is " + getLockNode);
		if (myZnode.equals(thisLock + "/" + getLockNode)) {
			logger.info("i am " + myZnode + ",i am getLock my node i");
			retVal = pjp.proceed();
		} else {
			waitForLock(getLockNode);
		}
		return retVal;
	}

	/**
	 * 抛出异常后，删掉自己，移除listener
	 * 
	 * @param joinpoint
	 * @param ex
	 */
	@AfterThrowing(value = "jobAspect()", throwing = "ex")
	public void afterThrowingAdvice(JoinPoint joinpoint, Throwable ex) {
		logger.info("job 执行异常，将会更换节点执行");
		removeWatch();
		deleteMyZnode();
	}

	/**
	 * 程序正常结束后，后的锁的节点设置结束标志，然后移除watch。其余的节点监听children的变化，然后删掉自己。
	 * 
	 * @param joinpoint
	 * @param retVal
	 * @throws ZkException 
	 */
	@AfterReturning(value = "jobAspect()", returning = "retVal")
	public void afterReturningAdvice(JoinPoint joinpoint, String retVal) throws ZkException {
		logger.info("myZnode is "+myZnode+", thisLock is :"+thisLock+","+"getNode0 is "+getNode0());
		
		if (myZnode.equals(thisLock + "/" + getNode0())
				&& !zk.readData(thisLock).toString().contains("end")) {
			logger.info("going to setEndDate(");
			setEndDate();
		}
	}

	/**
	 * 获取注解中对方法的描述信息 用于job描述 long【dateDiff】
	 * 
	 * @param joinPoint
	 *            切点
	 * @return 方法描述
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public Object[] getZkJobAnnotationDescription(JoinPoint joinPoint)
			throws Exception {
		String targetName = joinPoint.getTarget().getClass().getName();
		String methodName = joinPoint.getSignature().getName();
		Object[] arguments = joinPoint.getArgs();
		Class targetClass = Class.forName(targetName);
		targetClass_ = targetClass;
		Method[] methods = targetClass.getMethods();
		long dateDiff = 24 * 60 * 60;
		ZkJobType zkjt = ZkJobType.INTERVAL;
		int zkJobCount=0;
		for (Method method : methods) {
			if(method.isAnnotationPresent(ZkJob.class))
			zkJobCount++;
		}
		if(zkJobCount>1){
			throw new ZkException("前置通知抛出异常，同一个class中有多个zkJob注解，可能遭遇同时执行的可能，建议拆分方法到单个类中。当前class为："+targetName);
		}
		for (Method method : methods) {
			if (method.getName().equals(methodName)) {
				targetMethod = method;
				thisLock = root + "/" + targetName + "." + method.getName();
				logger.info("thisLock got , it is" + thisLock);
				Class[] clazzs = method.getParameterTypes();
				if (clazzs.length == arguments.length) {
					dateDiff = method.getAnnotation(ZkJob.class).dateDiff();
					zkjt = method.getAnnotation(ZkJob.class).type();
					break;
				}
			}
		}
		return new Object[] { dateDiff, zkjt };
	}

	/**
	 * 设置监听
	 */
	void setWatch() {
		logger.info("i am " + myZnode + ",i am setWatch");
		// 订阅thisLock children的变化
		zk.subscribeChildChanges(thisLock, iZkChildListener);
		// 订阅thisLock data的变化
		zk.subscribeDataChanges(thisLock, iZkDataListener);
	}

	/**
	 * 取消监听
	 */
	void removeWatch() {
		logger.info("i am " + myZnode + ",i am removeWatch");
		// 取消订阅thisLock children的变化
		zk.unsubscribeChildChanges(thisLock, iZkChildListener);
		// 取消订阅thisLock data的变化
		zk.unsubscribeDataChanges(thisLock, iZkDataListener);
	}

	/**
	 * childlistener
	 */
	IZkChildListener iZkChildListener = new IZkChildListener() {

		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) throws Exception {

			String childs = "";
			if (currentChilds != null) {
				if (currentChilds.size() > 0) {
					for (String s : currentChilds) {
						childs += s;
					}
				}
			}
			logger.info("i am " + myZnode + ",childChanged，child列表是" + childs);
			if (zk.exists(parentPath)) {
				String data = zk.readData(parentPath);
				if (zkjt.equals(ZkJobType.ONETIME)) {
					if (!data.equals(date + "end")
							&& !getNode0().equals(getLockNode)) {
						// 重新洗牌
						logger.info("i am " + myZnode + ",i am 重新洗牌");
						// 重新执行一遍job，怎么去执行呢？怎样去触发spring的这个方法，只能获取bean，然后执行bean的方法
						Object o = context
								.getBean(String
										.valueOf(
												targetClass_.getSimpleName()
														.charAt(0))
										.toLowerCase()
										.concat(targetClass_.getSimpleName()
												.substring(1)));
						// 重新触发切面
						targetMethod.invoke(o);
					}
				}
				if (zkjt.equals(ZkJobType.INTERVAL)) {

					if (!data.toString().contains("end")
							&& !getNode0().equals(getLockNode)) {
						// 重新洗牌
						logger.info("i am " + myZnode + ",i am 重新洗牌");
						// 重新执行一遍job，怎么去执行呢？怎样去触发spring的这个方法，只能获取bean，然后执行bean的方法
						Object o = context
								.getBean(String
										.valueOf(
												targetClass_.getSimpleName()
														.charAt(0))
										.toLowerCase()
										.concat(targetClass_.getSimpleName()
												.substring(1)));
						targetMethod.invoke(o);
					}
				}
			}
		}
	};
	/**
	 * dataListener
	 */
	IZkDataListener iZkDataListener = new IZkDataListener() {
		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
			// TODO Auto-generated method stub
		}

		@Override
		public void handleDataChange(String dataPath, Object data)
				throws Exception {
			logger.info("i am " + myZnode + ",root DateChanged,data is "
					+ data.toString());
			// 结束等待
			if (zkjt.equals(ZkJobType.ONETIME)) {
				if (data.equals(date + "end")) {
					// 删掉自己
					deleteMyZnode();
					// 删除监听
					removeWatch();
				}
			}
			if (zkjt.equals(ZkJobType.INTERVAL)) {
				if (data.toString().contains("end")) {
					// 删掉自己
					deleteMyZnode();
					// 删除监听
					removeWatch();
				}
			}

		}
	};

	// 删掉自己
	void deleteMyZnode() {
		if (myZnode != null && zk.exists(myZnode)) {
			zk.delete(myZnode);
			logger.info("i am " + myZnode + ",i delete myNode:   " + myZnode);
			myZnode = null;
		}
	}

	/**
	 * 排序childnode，获取node【0】
	 * 
	 * @return
	 * @throws ZkException 
	 */
	String getNode0() throws ZkException {
		List<String> list = zk.getChildren(thisLock);
		String[] nodes = list.toArray(new String[list.size()]);
		Arrays.sort(nodes);
		if (list == null||list.size()<1||nodes.length==0) {
			logger.info("all node delete ,but job is still not finished,please check "
					+ "\r\n|________|"
					+ "\r\n          ||          "
					+ "\r\n--------------------");
			// 删掉自己
			setEndDate();
			deleteMyZnode();
			removeWatch();
			throw new ZkException("all node delete ,but job is still not finished,please check");
			//return null;
		}
		return nodes[0];
	}

	/**
	 * @param lower
	 *            node[0]的值
	 * @throws InterruptedException
	 */
	void waitForLock(String lower) throws InterruptedException {
		String classData = zk.readData(thisLock);
		if (!classData.contains("end") && zk.exists(thisLock + "/" + lower)) {
			logger.info("i am " + myZnode + ",i am waiting");
			// 结束程序，等待下次调用
			return;
		} else if (classData.contains("end")) {
			logger.info("i am " + myZnode + ",i am dead");
			return;
		}
	}

	/**
	 * 设置结束标志
	 */
	void setEndDate() {
		if (zkjt.equals(ZkJobType.INTERVAL)
				&& !zk.readData(thisLock).toString().contains("end")) {
			logger.info(" i am " + myZnode + ",setEndData");
			zk.writeData(thisLock,
					zk.readData(thisLock).toString().replace("start", "end"));
		}
		if (zkjt.equals(ZkJobType.ONETIME)
				&& !zk.readData(thisLock).toString().contains("end")) {
			logger.info(" i am " + myZnode + ",setEndData");
			zk.writeData(thisLock, date + "end");
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		context = applicationContext;
	}
}
