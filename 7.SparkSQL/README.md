# SparkSQL Homework2

## 问题

### 问题1 - 思考题:如何避免小文件问题

* 如何避免小文件问题?给出2~3种解决方案

#### 解答

* 使用`Repartitions` 或者 `Coalesce` RDD 操作来减少分区（题目2）
* SparkSQL优化中AQE动态调整分区数

### 问题2 - 实现Compact table command

* 要求：
添加compact table命令，用于合并小文件，例如表test1总共有50000个文件，每个1MB，通过该命令，合成为500个文件，每个约100MB。
* 语法：
`COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES];`
* 说明：

1. 如果添加partitionSpec，则只合并指定的partition目录的文件。
2. 如果不加into fileNum files，则把表中的文件合并成128MB大小。
3. 以上两个算附加要求，基本要求只需要完成以下功能：
`COMPACT TABLE test1 INTO 500 FILES;`

#### 思路

##### 关键实现技术点

* 重新计算分区数

```scala
val tablePartitionsNumber = Math.max((sparkSession.sessionState
      .executePlan(table.logicalPlan)
      .optimizedPlan.stats.sizeInBytes >> 17).toInt, 1)
```

* 根据分区数重写文件

最开始的想法是直接试一试 in-place 方式重写文件

```scala
    table.repartition(tablePartitionsNumber)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableId.table)
```

但是这样的方式在执行的时候会抛出以下错误：

```shell
spark-sql> compact table block_list;
21/09/26 14:17:30 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
21/09/26 14:17:30 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
21/09/26 14:17:30 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
Error in query: Cannot overwrite table default.block_list that is also being read from
```

所以一种解决思路就是创建一个中间表来存储原有的数据，然后将其改名为原表名。

```scala

    table.repartition(tablePartitionsNumber)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tmpTableName)

    val tmpTable = sparkSession.table(tmpTableName)

    tmpTable
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableId.table)

    sparkSession.sql(s"DROP TABLE $tmpTableName ;")
```

#### 完整实现

由于之前作业中已经实现过SparkSQL `SHOW VERSION` 命令，所以具体如何编译`SqlBase.g4` 及SparkSQL 代码步骤就不赘述了。  
下面是基于Spark源码，`branch-3.2` 分支最新代码（commitHash：`540e45c3cc`)的patch 文件：  

[Spark-COMPACT_FILE-patch file](compactTable/feat__SparkSQL_COMPACT_TABLE_command.patch)

#### 关于测试

* 由于数据集及环境问题，测试合并文件逻辑时将 `128MB` 配置改为 `128B`重新编译
* 创建测试表并插入测试数据（单机本地磁盘存储环境）

```sql
CREATE TABLE block_list(accountType INT, city string);
INSERT INTO block_list VALUES 
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
(1, "BJ1"),(2, "BJ2"),(3, "BJ3");
```

* 查看表文件

```shell
➜  spark git:(branch-3.2) ✗ ls -lh spark-warehouse/block_list
total 64
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00000-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00001-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00002-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00003-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00004-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00005-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00006-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 RichardLee  staff    90B Sep 26 15:51 part-00007-0067344d-07ec-4836-878f-549dea59047b-c000

```

##### Case 1 - 不指定 fileNumber

* spark-sql执行`COMPACT TABLE` 不指定 fileNumber

```shell
spark-sql> compact table block_list;
Compact table block_list finished

```

* 查看表文件

```shell
➜  spark git:(branch-3.2) ✗ ls -lh spark-warehouse/block_list
total 40
-rw-r--r--  1 RichardLee  staff     0B Sep 26 15:53 _SUCCESS
-rw-r--r--  1 RichardLee  staff   803B Sep 26 15:53 part-00000-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 RichardLee  staff   803B Sep 26 15:53 part-00001-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 RichardLee  staff   803B Sep 26 15:53 part-00002-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 RichardLee  staff   803B Sep 26 15:53 part-00003-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 RichardLee  staff   803B Sep 26 15:53 part-00004-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet

```

##### case2 - 指定 fileNumber （happy case）

* spark-sql执行`COMPACT TABLE` 指定 fileNumber 为正整数

```shell
spark-sql> compact table block_list into 2 files;
Compact table block_list finished
```

* 查看表文件

```shell
➜  spark git:(branch-3.2) ✗ ls -lh spark-warehouse/block_list             
total 16
-rw-r--r--  1 RichardLee  staff     0B Sep 26 16:19 _SUCCESS
-rw-r--r--  1 RichardLee  staff   801B Sep 26 16:19 part-00000-84d417a4-ad51-40bd-8131-3760b92ddf0b-c000.snappy.parquet
-rw-r--r--  1 RichardLee  staff   801B Sep 26 16:19 part-00001-84d417a4-ad51-40bd-8131-3760b92ddf0b-c000.snappy.parquet

```

##### case 3 - 指定fileNumber 为 0 （corner case）

* spark-sql执行`COMPACT TABLE` 指定 fileNumber 为 0

```shell
spark-sql> compact table block_list into 0 files;
Compact table block_list finished
```

* 查看表文件

```shell
➜  spark git:(branch-3.2) ✗ ls -lh spark-warehouse/block_list
total 8
-rw-r--r--  1 RichardLee  staff     0B Sep 26 16:25 _SUCCESS
-rw-r--r--  1 RichardLee  staff   821B Sep 26 16:25 part-00000-9c8bb996-f690-49b5-a842-f989e3b71a19-c000.snappy.parquet
```

### 问题3 - Insert命令自动合并小文件

* 我们讲过AQE可以自动调整reducer的个数，但是正常跑Insert命令不会自动合并小文件，例如insert into t1 select * from t2;
* 请加一条物理规则（Strategy），让Insert命令自动进行小文件合并(repartition)。（不用考虑bucket表，不用考虑Hive表）

#### 解题思路

TODO

参考资料： [InsertIntoTable Unary Logical Operator
](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/InsertIntoTable.html)

#### 实现

TODO

#### 测试

TODO
