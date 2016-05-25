package com.dooioo.zkjob;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * @param dateDiff is not necessary,default value is 24*60*60，type is long
 * just like System.currentTimeMillis()
 * <p>use as :@ZkJob(dateDiff=10000,type=ZkJobType.INTERVAL)</p>
 * <pre><code>
 * code is
 * 	ZkJobType type() default ZkJobType.INTERVAL;
	long dateDiff() default 24*60*60;
	</code></pre>
 * @author liuyang
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public  @interface ZkJob {
	//默认是一天
	ZkJobType type() default ZkJobType.INTERVAL;
	long dateDiff() default 24*60*60;

	
}
