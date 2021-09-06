# 自定义`spark-sql` SHOW VERSION命令

## 环境信息

* Java SDK 版本：

```shell
java version "1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```

* Spark 源码版本：

```shell
branch: master
commit hash: 6bd491ecb8
```

## 关键信息点

* 获取Java 版本信息

```scala
System.getProperty("java.specification.version")
```

* 获取Spark版本信息

```scala
sparkSession.sparkContext.version
```

## 修改文件及patch文件

### 修改（添加）文件

* /sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
* org.apache.spark.sql.execution.SparkSqlParser
* org.apache.spark.sql.execution.command.ShowVersionCommand

[patch_file](feat__Spark-SQL____Add_SHOW_VERSION_command.patch)

## 运行结果

```bash
➜  spark git:(master) ✗ bin/spark-sql -S
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
21/09/06 17:52:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/09/06 17:52:24 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
21/09/06 17:52:24 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
21/09/06 17:52:26 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
21/09/06 17:52:26 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore RichardLee@192.168.30.134
spark-sql> SHOW VERSION;
SparkVersion :  3.3.0-SNAPSHOT 
 Java Version:  1.8
spark-sql> 

```