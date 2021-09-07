# SparkSQL Optimize

## 建表语句

```sql
CREATE TABLE account ( accountId string, accountName string, age INT , type INT, country string);
```

```sql
CREATE TABLE block_list(accountType INT, country string);
```

## 构建一条SQL，同时apply下面三条优化规则

* CombineFilters
* CollapseProject
* BooleanSimplificatio

### 查询语句

```sql
SELECT
	a.accountName 
FROM
	( SELECT accountName, age FROM account WHERE 1 = 1 AND age > 10 ) a 
WHERE
	a.age < 100;
```

### Apply 优化规则

* PushDownPredicates(CombineFilters)
* ColumnPruning
* CollapseProject
* ConstantFolding
* BooleanSimplification

### 日志结果输出

```text
21/09/07 16:44:05 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Project [accountName#102]                                                                                                                                                           Project [accountName#102]
!+- Filter (age#103 < 100)                                                                                                                                                           +- Project [accountName#102, age#103]
!   +- Project [accountName#102, age#103]                                                                                                                                               +- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))
!      +- Filter ((1 = 1) AND (age#103 > 10))                                                                                                                                              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]
!         +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]   
           
21/09/07 16:44:05 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Project [accountName#102]                                                                                                                                                        Project [accountName#102]
!+- Project [accountName#102, age#103]                                                                                                                                            +- Project [accountName#102]
    +- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))                                                                                                                     +- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))
       +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]         +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]
           
21/09/07 16:44:05 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Project [accountName#102]                                                                                                                                                        Project [accountName#102]
!+- Project [accountName#102]                                                                                                                                                     +- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))
!   +- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))                                                                                                                     +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]
!      +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]   
           
21/09/07 16:44:05 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Project [accountName#102]                                                                                                                                                     Project [accountName#102]
!+- Filter (((1 = 1) AND (age#103 > 10)) AND (age#103 < 100))                                                                                                                  +- Filter ((true AND (age#103 > 10)) AND (age#103 < 100))
    +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]      +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]
           
21/09/07 16:44:05 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [accountName#102]                                                                                                                                                     Project [accountName#102]
!+- Filter ((true AND (age#103 > 10)) AND (age#103 < 100))                                                                                                                     +- Filter ((age#103 > 10) AND (age#103 < 100))
    +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]      +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#101, accountName#102, age#103], Partition Cols: []]
           

```

## 构建一条SQL，同时apply下面五条优化规则

* ConstantFolding
* PushDownPredicates
* ReplaceDistinctWithAggregate
* ReplaceExceptWithAntiJoin
* FoldablePropagation

### 样例查询语句

```sql
SELECT DISTINCT
	a.type,
	a.country 
FROM
	( SELECT accountId, age, type, country, Now() z FROM account WHERE age > 9+1 ORDER BY z ) a 
WHERE
	a.age < 100 EXCEPT
SELECT
	accountType,
	country 
FROM
	block_list;
```

### Applied 优化规则

* EliminateSorts
* InferFiltersFromConstraints
* RemoveNoopOperators
* CollapseProject
* ColumnPruning
* ConstantFolding
* FoldablePropagation
* PushDownPredicates
* ReplaceDistinctWithAggregate
* ReplaceExceptWithAntiJoin

### SQL执行日志

```text
21/09/07 17:30:01 WARN PlanChangeLogger: Batch Subquery has no effect.
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin ===
!Except false                                                                                                                                                                                                        Distinct
!:- Distinct                                                                                                                                                                                                         +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
!:  +- Project [type#169, country#170]                                                                                                                                                                                  :- Distinct
!:     +- Filter (age#168 < 100)                                                                                                                                                                                        :  +- Project [type#169, country#170]
!:        +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :     +- Filter (age#168 < 100)
!:           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :        +- Sort [z#165 ASC NULLS FIRST], true
!:              +- Filter (age#168 > (9 + 1))                                                                                                                                                                           :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!:                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- Filter (age#168 > (9 + 1))
!+- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                       :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!                                                                                                                                                                                                                       +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
!Distinct                                                                                                                                                                                                               Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                   +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
!   :- Distinct                                                                                                                                                                                                            :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                                  :  +- Project [type#169, country#170]
    :     +- Filter (age#168 < 100)                                                                                                                                                                                        :     +- Filter (age#168 < 100)
    :        +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :        +- Sort [z#165 ASC NULLS FIRST], true
    :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
    :              +- Filter (age#168 > (9 + 1))                                                                                                                                                                           :              +- Filter (age#168 > (9 + 1))
    :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                       +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Result of Batch Replace Operators ===
!Except false                                                                                                                                                                                                        Aggregate [type#169, country#170], [type#169, country#170]
!:- Distinct                                                                                                                                                                                                         +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
!:  +- Project [type#169, country#170]                                                                                                                                                                                  :- Aggregate [type#169, country#170], [type#169, country#170]
!:     +- Filter (age#168 < 100)                                                                                                                                                                                        :  +- Project [type#169, country#170]
!:        +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :     +- Filter (age#168 < 100)
!:           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :        +- Sort [z#165 ASC NULLS FIRST], true
!:              +- Filter (age#168 > (9 + 1))                                                                                                                                                                           :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!:                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- Filter (age#168 > (9 + 1))
!+- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                       :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!                                                                                                                                                                                                                       +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
          
21/09/07 17:30:01 WARN PlanChangeLogger: Batch Aggregate has no effect.
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                             Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                   +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                                  :  +- Project [type#169, country#170]
!   :     +- Filter (age#168 < 100)                                                                                                                                                                                        :     +- Sort [z#165 ASC NULLS FIRST], true
!   :        +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :        +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!   :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))
!   :              +- Filter (age#168 > (9 + 1))                                                                                                                                                                           :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!   :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                               :  +- Project [type#169, country#170]
    :     +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :     +- Sort [z#165 ASC NULLS FIRST], true
!   :        +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :        +- Project [type#169, country#170, z#165]
!   :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))                                                                                                                                                     :           +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!   :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!                                                                                                                                                                                                                       +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                             Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                   +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                                  :  +- Project [type#169, country#170]
    :     +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                            :     +- Sort [z#165 ASC NULLS FIRST], true
!   :        +- Project [type#169, country#170, z#165]                                                                                                                                                                     :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!   :           +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                                                       :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))
!   :              +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))                                                                                                                                                     :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!   :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                               :  +- Project [type#169, country#170]
!   :     +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
    :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                                                       :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
    :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))                                                                                                                                                     :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))
    :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                               :  +- Project [type#169, country#170]
    :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true                                                                                                                                                       :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
    :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                                                       :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]
!   :           +- Filter ((age#168 > (9 + 1)) AND (age#168 < 100))                                                                                                                                                     :           +- Filter ((age#168 > 10) AND (age#168 < 100))
    :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                               :  +- Project [type#169, country#170]
    :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true                                                                                                                                                       :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
!   :        +- Project [type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                                                       :        +- Project [type#169, country#170]
!   :           +- Filter ((age#168 > 10) AND (age#168 < 100))                                                                                                                                                          :           +- Project [type#169, country#170]
!   :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :              +- Filter ((age#168 > 10) AND (age#168 < 100))
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!                                                                                                                                                                                                                       +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                             Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                   +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Project [type#169, country#170]                                                                                                                                                                                  :  +- Project [type#169, country#170]
    :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true                                                                                                                                                          :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
    :        +- Project [type#169, country#170]                                                                                                                                                                            :        +- Project [type#169, country#170]
!   :           +- Project [type#169, country#170]                                                                                                                                                                         :           +- Filter ((age#168 > 10) AND (age#168 < 100))
!   :              +- Filter ((age#168 > 10) AND (age#168 < 100))                                                                                                                                                          :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!   :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       :- Aggregate [type#169, country#170], [type#169, country#170]
!   :  +- Project [type#169, country#170]                                                                                                                                                                               :  +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
!   :     +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true                                                                                                                                                       :     +- Project [type#169, country#170]
!   :        +- Project [type#169, country#170]                                                                                                                                                                         :        +- Filter ((age#168 > 10) AND (age#168 < 100))
!   :           +- Filter ((age#168 > 10) AND (age#168 < 100))                                                                                                                                                          :           +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!   :              +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                 
           
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Result of Batch Operator Optimization before Inferring Filters ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                             Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                                   +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                          :- Aggregate [type#169, country#170], [type#169, country#170]
!   :  +- Project [type#169, country#170]                                                                                                                                                                                  :  +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
!   :     +- Filter (age#168 < 100)                                                                                                                                                                                        :     +- Project [type#169, country#170]
!   :        +- Sort [z#165 ASC NULLS FIRST], true                                                                                                                                                                         :        +- Filter ((age#168 > 10) AND (age#168 < 100))
!   :           +- Project [accountId#166, age#168, type#169, country#170, 2021-09-07 17:30:01.293 AS z#165]                                                                                                               :           +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
!   :              +- Filter (age#168 > (9 + 1))                                                                                                                                                                           +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
!   :                 +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]   
!   +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                    
          
21/09/07 17:30:01 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
 Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                       Aggregate [type#169, country#170], [type#169, country#170]
 +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))                                                                                                                             +- Join LeftAnti, ((type#169 <=> accountType#171) AND (country#170 <=> country#172))
    :- Aggregate [type#169, country#170], [type#169, country#170]                                                                                                                                                    :- Aggregate [type#169, country#170], [type#169, country#170]
    :  +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true                                                                                                                                                       :  +- Sort [2021-09-07 17:30:01.293 ASC NULLS FIRST], true
    :     +- Project [type#169, country#170]                                                                                                                                                                         :     +- Project [type#169, country#170]
!   :        +- Filter ((age#168 > 10) AND (age#168 < 100))                                                                                                                                                          :        +- Filter (isnotnull(age#168) AND ((age#168 > 10) AND (age#168 < 100)))
    :           +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]      :           +- HiveTableRelation [`default`.`account`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountId#166, accountName#167, age#168, type#169, country#170], Partition Cols: []]
    +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]                                                 +- HiveTableRelation [`default`.`block_list`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [accountType#171, country#172], Partition Cols: []]
           
```
