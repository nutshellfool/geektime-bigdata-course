# Presto Homework

## HyperLogLog算法在Presto的应用
### 问题一
1. 搜索HyperLogLog算法相关内容，了解其原理，写出5条
HyperLogLog的用途或大数据场景下的实际案例。

#### 解答
HyperLogLog 在大数据上的应用场景主要还是对大规模数据集中的不重复元素近似计算上。  
常见的具体使用案例有：  
* Reddit 使用HyperLogLog进行帖子的UV统计 [View Counting at Reddit](https://www.redditinc.com/blog/view-counting-at-reddit/)  
* Google 使用HyperLogLog计算搜索查询次数 [HyperLogLog in Practice: Algorithmic Engineering of a
State of The Art Cardinality Estimation Algorithm](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/40671.pdf)
* Elasticsearch 使用HyperLogLog及 HyperLogLog++算法做大规模数据集的基数聚合[Count on Elasticsearch!](https://www.elastic.co/cn/blog/count-elasticsearch)
* AWS RedShift COUNT 函数在近似选项参数下使用的就是HyperLogLog算法 [Amazon Redshift -- COUNT function](https://docs.aws.amazon.com/redshift/latest/dg/r_COUNT.html)

* AWS RedShift 近期又提供了队HLL函数的支持 [Amazon Redshift -- HLL function](https://docs.aws.amazon.com/redshift/latest/dg/r_HLL_function.html) 与此同时官方还提供了一个统计订单数及库存分析的例子 [Use HyperLogLog for trend analysis with Amazon Redshift](https://aws.amazon.com/cn/blogs/big-data/use-hyperloglog-for-trend-analysis-with-amazon-redshift/)

#### 其他参考
* [HyperLogLog - Wikipedia](https://en.wikipedia.org/wiki/HyperLogLog)
* [Probabilistic Data Structures for Web Analytics and Data Mining - Highly Scalable Blog](https://highlyscalable.wordpress.com/2012/05/01/probabilistic-structures-web-analytics-data-mining/)
* [HyperLogLog in Presto: Faster cardinality estimation - Facebook Engineering](https://engineering.fb.com/2018/12/13/data-infrastructure/hyperloglog/)
* [My favorite algorithm (and data structure): HyperLogLog](https://odino.org/my-favorite-data-structure-hyperloglog/)

### 问题二 
2. 在本地docker环境或阿里云e-mapreduce环境进行SQL查询，
要求在Presto中使用HyperLogLog计算近似基数。（请自行创
建表并插入若干数据）

建表语句：  

```SQL
CREATE TABLE user_visits (
  user_id int,
  visit_date date
);

CREATE TABLE visit_summaries (
  visit_date date,
  hll varbinary
);

INSERT INTO user_visits VALUES 
(1, DATE('2021-09-01')), 
(2, DATE('2021-09-01')),
(2, DATE('2021-09-02')),
(3, DATE('2021-09-03')),
(3, DATE('2021-09-03')),
(4, DATE('2021-09-03')),
(5, DATE('2021-09-03')),
(6, DATE('2021-09-04')),
(7, DATE('2021-09-04')),
(1, DATE('2021-09-24')), 
(2, DATE('2021-09-24')),
(2, DATE('2021-09-24')),
(3, DATE('2021-09-24')),
(3, DATE('2021-09-24')),
(4, DATE('2021-09-24')),
(5, DATE('2021-09-24')),
(6, DATE('2021-09-24')),
(7, DATE('2021-09-24')),
(11, DATE('2021-09-25')), 
(12, DATE('2021-09-25')),
(12, DATE('2021-09-25')),
(13, DATE('2021-09-25')),
(13, DATE('2021-09-25')),
(14, DATE('2021-09-25')),
(15, DATE('2021-09-25')),
(16, DATE('2021-09-25')),
(17, DATE('2021-09-25')),
(21, DATE('2021-09-25')), 
(22, DATE('2021-09-25')),
(22, DATE('2021-09-25')),
(23, DATE('2021-09-25')),
(23, DATE('2021-09-25')),
(24, DATE('2021-09-25')),
(25, DATE('2021-09-25')),
(26, DATE('2021-09-25')),
(27, DATE('2021-09-25')),
(111, DATE('2021-09-25')), 
(112, DATE('2021-09-25')),
(112, DATE('2021-09-25')),
(113, DATE('2021-09-25')),
(113, DATE('2021-09-25')),
(114, DATE('2021-09-25')),
(115, DATE('2021-09-25')),
(116, DATE('2021-09-25')),
(117, DATE('2021-09-25')),
(121, DATE('2021-09-25')), 
(122, DATE('2021-09-25')),
(122, DATE('2021-09-25')),
(123, DATE('2021-09-25')),
(123, DATE('2021-09-25')),
(124, DATE('2021-09-25')),
(125, DATE('2021-09-25')),
(126, DATE('2021-09-25')),
(127, DATE('2021-09-25')),
(211, DATE('2021-09-25')), 
(212, DATE('2021-09-25')),
(212, DATE('2021-09-25')),
(213, DATE('2021-09-25')),
(213, DATE('2021-09-25')),
(214, DATE('2021-09-25')),
(215, DATE('2021-09-25')),
(216, DATE('2021-09-25')),
(217, DATE('2021-09-25')),
(221, DATE('2021-09-25')), 
(222, DATE('2021-09-25')),
(222, DATE('2021-09-25')),
(223, DATE('2021-09-25')),
(223, DATE('2021-09-25')),
(224, DATE('2021-09-25')),
(225, DATE('2021-09-25')),
(226, DATE('2021-09-25')),
(227, DATE('2021-09-25'))
;

INSERT INTO visit_summaries
SELECT visit_date, cast(approx_set(user_id) AS varbinary)
FROM user_visits
GROUP BY visit_date;
```

查询语句：  

```SQL
SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
FROM visit_summaries
WHERE visit_date >= current_date - interval '7' day;
```

Aliyun EMR 运行结果：  

运行日志：  
```shell
submitting...
submit the task successfully
awaiting in yarn for resources... applicationId:application_1629363142012_0402
2021-09-25 16:48:09.734 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### drwx--x--- hadoop container_1629363142012_0402_01_000001
2021-09-25 16:48:09.734 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop job-launcher-spec.json
2021-09-25 16:48:09.734 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop launcher.jar
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-job-presto-sql-3.0.8.0.1.jar
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop commons-text-1.6.jar
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rw-r----- hadoop container_tokens
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop logback-core-1.2.3.jar
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rw-r----- hadoop .container_tokens.crc
2021-09-25 16:48:09.735 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop slf4j-api-1.7.25.jar
2021-09-25 16:48:09.736 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop yarn-site.xml
2021-09-25 16:48:09.736 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop logback-classic-1.2.3.jar
2021-09-25 16:48:09.736 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-codec-3.0.8.0.1.jar
2021-09-25 16:48:09.736 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### drwx--x--- hadoop tmp
2021-09-25 16:48:09.736 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop mapred-site.xml
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop hdfs-site.xml
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop gson-2.8.2.jar
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-job-launcher-spi-3.0.8.0.1.jar
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rw-r----- hadoop .launch_container.sh.crc
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop launcher-am-conf.xml
2021-09-25 16:48:09.737 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rwx------ hadoop default_container_executor_session.sh
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop job-launcher-classpath
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rwx------ hadoop default_container_executor.sh
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-job-common-shell-3.0.8.0.1.jar
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop commons-lang3-3.7.jar
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop job.metadata
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-job-hadoop-jar-3.0.8.0.1.jar
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop core-site.xml
2021-09-25 16:48:09.738 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-job-spark-3.0.8.0.1.jar
2021-09-25 16:48:09.739 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rw-r----- hadoop .default_container_executor_session.sh.crc
2021-09-25 16:48:09.739 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop flow-agent-common-3.0.8.0.1.jar
2021-09-25 16:48:09.739 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -r-x------ hadoop logback.xml
2021-09-25 16:48:09.739 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rw-r----- hadoop .default_container_executor.sh.crc
2021-09-25 16:48:09.739 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### -rwx------ hadoop launch_container.sh

=================LIST WORK DIR END=================

2021-09-25 16:48:10.242 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - ### Launcher log rolling, enabled: false, max size: 52428800, max file num: 10
2021-09-25 16:48:10.242 [main] INFO  c.a.e.f.a.j.launcher.impl.PrestoSqlJobLauncherImpl - Using presto cli executable: /usr/lib/presto-current/bin/presto ...
2021-09-25 16:48:10.242 [main] INFO  c.a.e.f.a.j.launcher.impl.PrestoSqlJobLauncherImpl - Refactor job arguments: [presto, -e, SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
FROM visit_summaries
WHERE visit_date >= current_date - interval '7' day;] -> [/usr/lib/presto-current/bin/presto, --schema, default, --catalog, hive, --server, emr-header-1:9090, --output-format, TSV_HEADER, -e, SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
FROM visit_summaries
WHERE visit_date >= current_date - interval '7' day;]
2021-09-25 16:48:10.242 [main] INFO  c.a.e.f.a.j.launcher.impl.PrestoSqlJobLauncherImpl - Create execution context for presto sql job ...
2021-09-25 16:48:10.243 [main] INFO  c.a.e.f.a.j.launcher.impl.PrestoSqlJobLauncherImpl - There're 1 sqls, by pass=false.
Sat Sep 25 16:48:10 CST 2021 [JobLauncherRunner] INFO Do job launching ...
2021-09-25 16:48:10.249 [main] INFO  c.a.emr.flow.agent.jobs.launcher.JobLauncherBase - [FJI-A910902EB5EBDDA4_0] Launching ...
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] work dir: /mnt/disk2/yarn/usercache/hadoop/appcache/application_1629363142012_0402/container_1629363142012_0402_01_000001
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] log dir: /mnt/disk4/log/hadoop-yarn/containers/application_1629363142012_0402/container_1629363142012_0402_01_000001
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] current user: hadoop
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] submit user: hadoop
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] envs(override): {EMR_FLOW_AGENT_JOB_ID=FJI-A910902EB5EBDDA4_0, PATH=/mnt/disk2/yarn/usercache/hadoop/appcache/application_1629363142012_0402/container_1629363142012_0402_01_000001, EMR_FLOW_CLUSTER_ID=C-14461BCBBEF08246, FLOW_SKIP_SQL_ANALYZE=true, EMR_FLOW_JOB_INSTANCE_ID=FJI-A910902EB5EBDDA4, EMR_FLOW_NODE_INSTANCE_ID=FJI-A910902EB5EBDDA4, EMR_FLOW_JOB_ID=FJ-D7C2163BD52A6681}
2021-09-25 16:48:10.252 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] Executing command line: [/usr/lib/presto-current/bin/presto, --schema, default, --catalog, hive, --server, emr-header-1:9090, --output-format, TSV_HEADER, -f, /mnt/disk2/yarn/usercache/hadoop/appcache/application_1629363142012_0402/container_1629363142012_0402_01_000001/_sql_0.sql]
2021-09-25 16:48:10.253 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] Shell Executor type: com.aliyun.emr.flow.agent.common.shell.JavaShellExecutor.


=================JOB OUTPUT BEGIN=================

2021-09-25 16:48:11.999 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [COMMAND][FJI-A910902EB5EBDDA4_0.0] Finished command line, exit code=0.
Sat Sep 25 16:48:12 CST 2021 [JobLauncherRunner] INFO Closing job launcher ...
2021-09-25 16:48:12.001 [main] INFO  c.a.emr.flow.agent.jobs.launcher.JobLauncherBase - [FJI-A910902EB5EBDDA4_0] Closing ...
2021-09-25 16:48:12.001 [main] INFO  c.a.e.f.a.j.l.impl.CommonShellJobLauncherImpl - [FJI-A910902EB5EBDDA4_0] Stopping command executor ...
Sat Sep 25 16:48:12 CST 2021 [YarnJobLauncherAM] INFO Closing launcher am ...
2021-09-25 16:48:12.004 [main] INFO  o.a.hadoop.yarn.client.api.impl.AMRMClientImpl - Waiting for application to be successfully unregistered.
2021-09-25 16:48:12.220 [Shutdown-FJI-A910902EB5EBDDA4_0] INFO  c.a.emr.flow.agent.jobs.launcher.JobLauncherBase - [FJI-A910902EB5EBDDA4_0] Call shutdown hook.
2021-09-25 16:48:12.220 [Shutdown-FJI-A910902EB5EBDDA4_0] INFO  c.a.emr.flow.agent.jobs.launcher.JobLauncherBase - [FJI-A910902EB5EBDDA4_0] Closing ...
2021-09-25 16:48:12.220 [Shutdown-FJI-A910902EB5EBDDA4_0] INFO  c.a.emr.flow.agent.jobs.launcher.JobLauncherBase - [FJI-A910902EB5EBDDA4_0] This launcher is closed already, skip.

######END_OF_LOG######
```

运行结果(json)：  

```json
[
    [
        "weekly_unique_users"
    ],
    [
        "49"
    ]
]
```

实际精确结果：46 
HyperLogLog 结果与实际精确结果在数据量较小的数据集中存在一定误差，在大数据集中结果待验证，另外还未找到高效倒入大批数据到PrestoSQL的方法，这部分在找到高效方法后验证。  

3. 学习使用Presto-Jdbc库连接docker或e-mapreduce环境，重
复上述查询。（选做）