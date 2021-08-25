# Spark Inverted-index

## Prepare for develop environment
* Built spark [spark 3.1.2](https://www.apache.org/dyn/closer.lua/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz)
* IntelliJ IDEA  
  with [scala plugin](https://plugins.jetbrains.com/plugin/1347-scala)  -- for scala language  
  and [bigdata tools plugin](https://plugins.jetbrains.com/plugin/12494-big-data-tools) -- for spark-submit 
* Apache Maven 3.*
* JVM 8+

## General Information

this repository is a Spark project for Inverted-index application:

* com.aibyte.bigdata.spark.examples
    * [SparkRDDInvertedIndex.scala](src/main/scala/com/aibyte/bigdata/spark/examples/SparkRDDInvertedIndex.scala)
      Code to build Inverted-index

## Execution example

### local dev env

#### input file(s) example

under path [src/main/resources/text](src/main/resources/text)  
three `.txt `files:

* `1.txt`

```text
it is what it is
guess what is this
```

* `2.txt`

```text
what is it
```

* `3.txt`

```text
it is a banana
```

#### Output result example

output directory tree:

```text
output
├── _SUCCESS
└── part-00000
```

part-00000

```text
(this,(1.txt,1))
(banana,(3.txt,1))
(it,(3.txt,1)),(it,(1.txt,2)),(it,(2.txt,1))
(is,(3.txt,1)),(is,(2.txt,1)),(is,(1.txt,3))
(a,(3.txt,1))
(guess,(1.txt,1))
(what,(1.txt,2)),(what,(2.txt,1))
```

#### spark-submit log

```text
21/08/25 09:59:40 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
21/08/25 09:59:40 INFO SparkContext: Running Spark version 3.1.2
21/08/25 09:59:40 INFO ResourceUtils: ==============================================================
21/08/25 09:59:40 INFO ResourceUtils: No custom resources configured for spark.driver.
21/08/25 09:59:40 INFO ResourceUtils: ==============================================================
21/08/25 09:59:40 INFO SparkContext: Submitted application: lirui-SparkCoreRDDInvertedIndex
21/08/25 09:59:40 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
21/08/25 09:59:40 INFO ResourceProfile: Limiting resource is cpu
21/08/25 09:59:40 INFO ResourceProfileManager: Added ResourceProfile id: 0
21/08/25 09:59:40 INFO SecurityManager: Changing view acls to: RichardLee
21/08/25 09:59:40 INFO SecurityManager: Changing modify acls to: RichardLee
21/08/25 09:59:40 INFO SecurityManager: Changing view acls groups to: 
21/08/25 09:59:40 INFO SecurityManager: Changing modify acls groups to: 
21/08/25 09:59:40 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(RichardLee); groups with view permissions: Set(); users  with modify permissions: Set(RichardLee); groups with modify permissions: Set()
21/08/25 09:59:41 INFO Utils: Successfully started service 'sparkDriver' on port 62805.
21/08/25 09:59:41 INFO SparkEnv: Registering MapOutputTracker
21/08/25 09:59:41 INFO SparkEnv: Registering BlockManagerMaster
21/08/25 09:59:41 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/08/25 09:59:41 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/08/25 09:59:41 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
21/08/25 09:59:41 INFO DiskBlockManager: Created local directory at /private/var/folders/jn/41x0x_9955d9w0w83r6xy4rw0000gp/T/blockmgr-4aabfe2d-b0d6-46bf-8454-bab01a0fc742
21/08/25 09:59:41 INFO MemoryStore: MemoryStore started with capacity 366.3 MiB
21/08/25 09:59:41 INFO SparkEnv: Registering OutputCommitCoordinator
21/08/25 09:59:41 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
21/08/25 09:59:41 INFO Utils: Successfully started service 'SparkUI' on port 4041.
21/08/25 09:59:41 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.30.75:4041
21/08/25 09:59:41 INFO SparkContext: Added JAR file:/Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/target/RDD-inverted-index-1.0-SNAPSHOT.jar at spark://192.168.30.75:62805/jars/RDD-inverted-index-1.0-SNAPSHOT.jar with timestamp 1629856780660
21/08/25 09:59:41 INFO Executor: Starting executor ID driver on host 192.168.30.75
21/08/25 09:59:41 INFO Executor: Fetching spark://192.168.30.75:62805/jars/RDD-inverted-index-1.0-SNAPSHOT.jar with timestamp 1629856780660
21/08/25 09:59:41 INFO TransportClientFactory: Successfully created connection to /192.168.30.75:62805 after 27 ms (0 ms spent in bootstraps)
21/08/25 09:59:41 INFO Utils: Fetching spark://192.168.30.75:62805/jars/RDD-inverted-index-1.0-SNAPSHOT.jar to /private/var/folders/jn/41x0x_9955d9w0w83r6xy4rw0000gp/T/spark-5c1aa502-cbc8-4416-8fdc-a1cf68735feb/userFiles-f59a329c-f9be-4520-a507-3086637e9b71/fetchFileTemp4355794021825349911.tmp
21/08/25 09:59:41 INFO Executor: Adding file:/private/var/folders/jn/41x0x_9955d9w0w83r6xy4rw0000gp/T/spark-5c1aa502-cbc8-4416-8fdc-a1cf68735feb/userFiles-f59a329c-f9be-4520-a507-3086637e9b71/RDD-inverted-index-1.0-SNAPSHOT.jar to class loader
21/08/25 09:59:41 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 62807.
21/08/25 09:59:41 INFO NettyBlockTransferService: Server created on 192.168.30.75:62807
21/08/25 09:59:41 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/08/25 09:59:41 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.30.75, 62807, None)
21/08/25 09:59:41 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.30.75:62807 with 366.3 MiB RAM, BlockManagerId(driver, 192.168.30.75, 62807, None)
21/08/25 09:59:41 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.30.75, 62807, None)
21/08/25 09:59:41 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.30.75, 62807, None)
21/08/25 09:59:42 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 306.8 KiB, free 366.0 MiB)
21/08/25 09:59:42 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 27.1 KiB, free 366.0 MiB)
21/08/25 09:59:42 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.30.75:62807 (size: 27.1 KiB, free: 366.3 MiB)
21/08/25 09:59:42 INFO SparkContext: Created broadcast 0 from wholeTextFiles at SparkRDDInvertedIndex.scala:27
21/08/25 09:59:42 INFO FileInputFormat: Total input files to process : 3
21/08/25 09:59:42 INFO FileInputFormat: Total input files to process : 3
21/08/25 09:59:42 INFO deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
21/08/25 09:59:42 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
21/08/25 09:59:42 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
21/08/25 09:59:42 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
21/08/25 09:59:43 INFO SparkContext: Starting job: runJob at SparkHadoopWriter.scala:83
21/08/25 09:59:43 INFO DAGScheduler: Registering RDD 3 (map at SparkRDDInvertedIndex.scala:33) as input to shuffle 1
21/08/25 09:59:43 INFO DAGScheduler: Registering RDD 6 (groupBy at SparkRDDInvertedIndex.scala:42) as input to shuffle 0
21/08/25 09:59:43 INFO DAGScheduler: Got job 0 (runJob at SparkHadoopWriter.scala:83) with 1 output partitions
21/08/25 09:59:43 INFO DAGScheduler: Final stage: ResultStage 2 (runJob at SparkHadoopWriter.scala:83)
21/08/25 09:59:43 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
21/08/25 09:59:43 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
21/08/25 09:59:43 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at map at SparkRDDInvertedIndex.scala:33), which has no missing parents
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 6.1 KiB, free 366.0 MiB)
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.4 KiB, free 366.0 MiB)
21/08/25 09:59:43 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.30.75:62807 (size: 3.4 KiB, free: 366.3 MiB)
21/08/25 09:59:43 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1388
21/08/25 09:59:43 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at map at SparkRDDInvertedIndex.scala:33) (first 15 tasks are for partitions Vector(0))
21/08/25 09:59:43 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
21/08/25 09:59:43 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192.168.30.75, executor driver, partition 0, PROCESS_LOCAL, 4946 bytes) taskResourceAssignments Map()
21/08/25 09:59:43 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
21/08/25 09:59:43 INFO WholeTextFileRDD: Input split: Paths:/Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/src/main/resources/text/1.txt:0+35,/Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/src/main/resources/text/2.txt:0+10,/Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/src/main/resources/text/3.txt:0+14
21/08/25 09:59:43 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1334 bytes result sent to driver
21/08/25 09:59:43 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 541 ms on 192.168.30.75 (executor driver) (1/1)
21/08/25 09:59:43 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/08/25 09:59:43 INFO DAGScheduler: ShuffleMapStage 0 (map at SparkRDDInvertedIndex.scala:33) finished in 0.671 s
21/08/25 09:59:43 INFO DAGScheduler: looking for newly runnable stages
21/08/25 09:59:43 INFO DAGScheduler: running: Set()
21/08/25 09:59:43 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
21/08/25 09:59:43 INFO DAGScheduler: failed: Set()
21/08/25 09:59:43 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[6] at groupBy at SparkRDDInvertedIndex.scala:42), which has no missing parents
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 6.3 KiB, free 366.0 MiB)
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 3.3 KiB, free 366.0 MiB)
21/08/25 09:59:43 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.30.75:62807 (size: 3.3 KiB, free: 366.3 MiB)
21/08/25 09:59:43 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1388
21/08/25 09:59:43 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[6] at groupBy at SparkRDDInvertedIndex.scala:42) (first 15 tasks are for partitions Vector(0))
21/08/25 09:59:43 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
21/08/25 09:59:43 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (192.168.30.75, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
21/08/25 09:59:43 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
21/08/25 09:59:43 INFO ShuffleBlockFetcherIterator: Getting 1 (334.0 B) non-empty blocks including 1 (334.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) remote blocks
21/08/25 09:59:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
21/08/25 09:59:43 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1463 bytes result sent to driver
21/08/25 09:59:43 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 61 ms on 192.168.30.75 (executor driver) (1/1)
21/08/25 09:59:43 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/08/25 09:59:43 INFO DAGScheduler: ShuffleMapStage 1 (groupBy at SparkRDDInvertedIndex.scala:42) finished in 0.072 s
21/08/25 09:59:43 INFO DAGScheduler: looking for newly runnable stages
21/08/25 09:59:43 INFO DAGScheduler: running: Set()
21/08/25 09:59:43 INFO DAGScheduler: waiting: Set(ResultStage 2)
21/08/25 09:59:43 INFO DAGScheduler: failed: Set()
21/08/25 09:59:43 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[9] at saveAsTextFile at SparkRDDInvertedIndex.scala:48), which has no missing parents
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 88.3 KiB, free 365.9 MiB)
21/08/25 09:59:43 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 31.9 KiB, free 365.8 MiB)
21/08/25 09:59:43 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.30.75:62807 (size: 31.9 KiB, free: 366.2 MiB)
21/08/25 09:59:43 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1388
21/08/25 09:59:43 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[9] at saveAsTextFile at SparkRDDInvertedIndex.scala:48) (first 15 tasks are for partitions Vector(0))
21/08/25 09:59:43 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks resource profile 0
21/08/25 09:59:43 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2) (192.168.30.75, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
21/08/25 09:59:43 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
21/08/25 09:59:43 INFO ShuffleBlockFetcherIterator: Getting 1 (405.0 B) non-empty blocks including 1 (405.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) remote blocks
21/08/25 09:59:43 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
21/08/25 09:59:43 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
21/08/25 09:59:43 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
21/08/25 09:59:43 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
21/08/25 09:59:43 INFO FileOutputCommitter: Saved output of task 'attempt_202108250959427798502487993155033_0009_m_000000_0' to file:/Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/target/output/_temporary/0/task_202108250959427798502487993155033_0009_m_000000
21/08/25 09:59:43 INFO SparkHadoopMapRedUtil: attempt_202108250959427798502487993155033_0009_m_000000_0: Committed
21/08/25 09:59:43 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 1631 bytes result sent to driver
21/08/25 09:59:43 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 120 ms on 192.168.30.75 (executor driver) (1/1)
21/08/25 09:59:43 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
21/08/25 09:59:44 INFO DAGScheduler: ResultStage 2 (runJob at SparkHadoopWriter.scala:83) finished in 0.153 s
21/08/25 09:59:44 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
21/08/25 09:59:44 INFO TaskSchedulerImpl: Killing all running tasks in stage 2: Stage finished
21/08/25 09:59:44 INFO DAGScheduler: Job 0 finished: runJob at SparkHadoopWriter.scala:83, took 0.977705 s
21/08/25 09:59:44 INFO SparkHadoopWriter: Job job_202108250959427798502487993155033_0009 committed.
21/08/25 09:59:44 INFO SparkUI: Stopped Spark web UI at http://192.168.30.75:4041
21/08/25 09:59:44 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/08/25 09:59:44 INFO MemoryStore: MemoryStore cleared
21/08/25 09:59:44 INFO BlockManager: BlockManager stopped
21/08/25 09:59:44 INFO BlockManagerMaster: BlockManagerMaster stopped
21/08/25 09:59:44 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/08/25 09:59:44 INFO SparkContext: Successfully stopped SparkContext
21/08/25 09:59:44 INFO ShutdownHookManager: Shutdown hook called
21/08/25 09:59:44 INFO ShutdownHookManager: Deleting directory /private/var/folders/jn/41x0x_9955d9w0w83r6xy4rw0000gp/T/spark-d1229252-9add-434c-9549-ec13a0fcc65a
21/08/25 09:59:44 INFO ShutdownHookManager: Deleting directory /private/var/folders/jn/41x0x_9955d9w0w83r6xy4rw0000gp/T/spark-5c1aa502-cbc8-4416-8fdc-a1cf68735feb

Process finished with exit code 0

```

### EMR production env
TODO
