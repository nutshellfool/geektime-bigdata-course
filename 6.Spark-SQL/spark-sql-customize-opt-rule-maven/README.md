# 自定义优化规则

## 创建工程

* 创建Maven 工程，并添加Scala 作为框架支持（`Add Framework Support`）
* 添加必要的依赖
```xml
  <dependencies>
  <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>3.1.1</version>
  </dependency>
  <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.1.1</version>
  </dependency>
</dependencies>
```
* 添加必要的构建插件：  
```xml
  <build>
    <outputDirectory>target/scala-${scala.binary.version}/classes</outputDirectory>
    <testOutputDirectory>target/scala-${scala.binary.version}/test-classes</testOutputDirectory>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-deploy-plugin</artifactId>
        <configuration>
          <skip>true</skip>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-install-plugin</artifactId>
        <configuration>
          <skip>true</skip>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <configuration>
          <outputDirectory>${jars.target.dir}</outputDirectory>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.scala-tools</groupId>
        <artifactId>maven-scala-plugin</artifactId>
        <version>2.15.2</version>
        <executions>
          <execution>
            <id>scala-compile</id>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

* 实现自定义优化规则
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case _ => println("My push down rule applied")
      plan
  }
}
```
* 创建自定义Extension
```scala

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      MyPushDown(session)
    }
  }
}
```
## 编译工程

```shell
mvn clean package
```
## 提交运行
* `spark-sql`中设置planChangeLog级别 `set spark.sql.planChangeLog.level=WARN;`

* 重新执行`spark-sql`
```shell
bin/spark-sql --jars  file:///<abusolute_path_to_jar>/spark-sql-customize-opt-rule-maven/target/spark-sql-customize-opt-rule-maven-1.0-SNAPSHOT.jar --conf spark.sql.extensions=com.aibyte.bigdata.spark.examples.MySparkSessionExtension
```
* 执行任意查询
```sql
spark-sql> select * from account;
```
## 运行日志
```text
➜  spark git:(master) ✗ bin/spark-sql --jars  file:///xxxxxxxxx/spark-sql-customize-opt-rule-maven/target/spark-sql-customize-opt-rule-maven-1.0-SNAPSHOT.jar --conf spark.sql.extensions=com.aibyte.bigdata.spark.examples.MySparkSessionExtension
21/09/08 09:33:44 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
21/09/08 09:33:48 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
21/09/08 09:33:48 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
21/09/08 09:33:50 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
21/09/08 09:33:50 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore RichardLee@192.168.30.134
Spark master: local[*], Application Id: local-1631064826729
spark-sql> select * from account;
My push down rule applied
My push down rule applied
21/09/08 09:33:59 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
Time taken: 3.788 seconds
spark-sql> 
```