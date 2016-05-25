# zk-job
One Way Of Distributed Job Scheduling for Java

使用zookeeper作为注册中心，实现分布式锁机制，调度分布式spring task。实现多个节点同一个job只有一个节点running，其余节
点处于监听等待状态，一旦running的节点执行异常，从等待的节点中选出一个节点重新执行task。或者等running节点正常结束，移除监听。

采用spring aop 方式，使用自定义注解实现切面功能。
