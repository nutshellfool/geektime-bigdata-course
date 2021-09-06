# SparkSQL homework

## 1. 为Spark SQL添加一条自定义命令  

• SHOW VERSION;  
• 显示当前Spark版本和Java版本

### 解答1

[SHOW VERSION 解答](1-SHOW_VERSION/README.md)

## 2. 构建SQL满足如下要求

通过`set spark.sql.planChangeLog.level=WARN;`查看

* 构建一条SQL，同时apply下面三条优化规则:
  * CombineFilters
  * CollapseProject
  * BooleanSimplification
* 构建一条SQL，同时apply下面五条优化规则:
  * ConstantFolding
  * PushDownPredicates
  * ReplaceDistinctWithAggregate
  * ReplaceExceptWithAntiJoin
  * FoldablePropagation

### 解答2

## 3. 实现自定义优化规则(静默规则)

第一步 实现自定义规则(静默规则，通过set spark.sql.planChangeLog.level=WARN;确认执行到就行)

```scala
case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
def apply(plan: LogicalPlan): LogicalPlan = plan transform { .... } }
```

第二步 创建自己的Extension并注入 

```scala
class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
override def apply(extensions: SparkSessionExtensions): Unit = { extensions.injectOptimizerRule { session =>
new MyPushDown(session) }
} }
```

第三步 通过spark.sql.extensions提交

`bin/spark-sql --jars my.jar --conf spark.sql.extensions=com.jikeshijian.MySparkSessionExtension`  

### 解答3
