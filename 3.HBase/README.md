# HBase Java API example

## CRUD Operations in JavaAPI & HBase shell
### Create namespace
* Java
```java
NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(NAMESPACE).build();
admin.createNamespace(namespaceDescriptor);
```

* Shell command:
```shell
hbase(main):006:0> create_namespace 'lirui_test'
```
```shell
Took 0.2614 seconds
```

### Create/delete table
* Java
```java
TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
    .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_NAME.getBytes()).build())
    .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_INFO.getBytes()).build())
    .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_SCORE.getBytes()).build())
    .build();
// delete table first
if (admin.tableExists(TABLE_NAME) && admin.isTableDisabled(TABLE_NAME)) {
  // disable table before delete it
  admin.disableTable(TABLE_NAME);
  admin.deleteTable(TABLE_NAME);
}

admin.createTable(tableDescriptor);
```
* Shell command:
```shell
hbase(main):006:0> create 'lirui_test:student', 'name' ,'info', 'score'
```
```shell
Created table lirui_test:student
Took 2.2679 seconds
=> Hbase::Table - lirui_test:student
hbase(main):008:0> list 'lirui_test:student'
TABLE
lirui_test:student
1 row(s)
Took 0.0122 seconds
=> ["lirui_test:student"]
```
### Insert Data
* Java
1. Implicit Version
```java
Put put = new Put(CF_NAME_ROW_VALUE);// lirui
put.addColumn(COLUMN_FAMILY_NAME_NAME.getBytes(), null, CF_NAME_ROW_VALUE);
put.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_STUDENT_ID_ROW,
    CF_INFO_STUDENT_ID_ROW_VALUE);
put.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_CLASS_ROW, CF_INFO_CLASS_ROW_VALUE);
put.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_UNDERSTANDING_ROW,
    CF_SCORE_UNDERSTANDING_ROW_VALUE);
put.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_PROGRAMMING_ROW,
    CF_SCORE_PROGRAMMING_ROW_VALUE);
table.put(put);
```
2. Cell Version
```java
Put put = new Put(CF_NAME_ROW_VALUE_TOM);
put.add(
CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
.setType(Type.Put)
.setRow(CF_NAME_ROW_VALUE_TOM)
.setFamily(COLUMN_FAMILY_NAME_NAME.getBytes())
.setValue(CF_NAME_ROW_VALUE_TOM)
.build());
put.add(
CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
.setType(Type.Put)
.setRow(CF_NAME_ROW_VALUE_TOM)
.setQualifier(CF_INFO_STUDENT_ID_ROW)
.setFamily(COLUMN_FAMILY_NAME_INFO.getBytes())
.setValue(CF_INFO_STUDENT_ID_ROW_VALUE_TOM)
.build());
put.add(
CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
.setType(Type.Put)
.setRow(CF_NAME_ROW_VALUE_TOM)
.setQualifier(CF_INFO_CLASS_ROW)
.setFamily(COLUMN_FAMILY_NAME_INFO.getBytes())
.setValue(CF_INFO_CLASS_ROW_VALUE_TOM)
.build());
put.add(
CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
.setType(Type.Put)
.setRow(CF_NAME_ROW_VALUE_TOM)
.setQualifier(CF_SCORE_PROGRAMMING_ROW)
.setFamily(COLUMN_FAMILY_NAME_SCORE.getBytes())
.setValue(CF_SCORE_PROGRAMMING_ROW_VALUE_TOM)
.build());
put.add(
CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
.setType(Type.Put)
.setRow(CF_NAME_ROW_VALUE_TOM)
.setQualifier(CF_SCORE_UNDERSTANDING_ROW)
.setFamily(COLUMN_FAMILY_NAME_SCORE.getBytes())
.setValue(CF_SCORE_UNDERSTANDING_ROW_VALUE_TOM)
.build());
table.put(put);
```

* Shell command:
```shell
hbase(main):016:0> put 'lirui_test:student', 'Tom', 'name', 'Tom'
Took 0.0159 seconds
hbase(main):011:0> put 'lirui_test:student', 'Tom', 'info:student_id', '20210000000001'
Took 1.0948 seconds
hbase(main):012:0> put 'lirui_test:student', 'Tom', 'info:class', '1'
Took 0.0084 seconds
hbase(main):013:0> put 'lirui_test:student', 'Tom', 'score:understanding', '75'
Took 0.0069 seconds
hbase(main):014:0> put 'lirui_test:student', 'Tom', 'score:programming', '82'
Took 0.0061 seconds
```

### Fetch Data
```java
Get get = new Get(CF_NAME_ROW_VALUE);
Result result = table.get(get);
```
* Shell command:
```shell
get 'lirui_test:student', 'Tom'
```
```shell
COLUMN                                  CELL
 info:class                             timestamp=1627454988710, value=1
 info:student_id                        timestamp=1627454937674, value=20210000000001
 name:                                  timestamp=1627455464469, value=Tom
 score:programming                      timestamp=1627455051435, value=82
 score:understanding                    timestamp=1627455032761, value=75
1 row(s)
Took 0.0170 seconds
```

### Delete Data
* Java
```java
Delete delete = new Delete(CF_NAME_ROW_VALUE_TOM);
delete.addColumn(COLUMN_FAMILY_NAME_NAME.getBytes(), null);
delete.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), null);
delete.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), null);
table.delete(delete);
```

* Shell command:
```shell
hbase(main):019:0> disable 'lirui_test:student'
Took 1.3026 seconds
```
```shell
hbase(main):020:0> drop 'lirui_test:student'
Took 0.5012 seconds
```

## How to Run it in production server?

1. Compile and package the jar
```shell
mvn clean package
```
2. Copy jar file to server

3. Run it on server
```shell
java -jar hbase-api-1.0-SNAPSHOT.jar
```

## The output of running application
```shell
1117 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - run ------>
1118 [main] INFO  org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient  - Connect 0x740cae06 to 47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181 with session timeout=90000ms, retries 30, retry interval 1000ms, keepAlive=60000ms
1120 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06] INFO  org.apache.zookeeper.ZooKeeper  - Initiating client connection, connectString=47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181 sessionTimeout=90000 watcher=org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient$$Lambda$12/495601099@2501d955
1121 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] INFO  org.apache.zookeeper.ClientCnxn  - Opening socket connection to server 47.101.216.12/47.101.216.12:2181. Will not attempt to authenticate using SASL (unknown error)
1140 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x0a38d7a3-SendThread(47.101.206.249:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - Reading reply sessionid:0x17a8fa62b6c9103, packet:: clientPath:null serverPath:null finished:false header:: 3,-11  replyHeader:: 3,8590371657,0  request:: null response:: null
1140 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] INFO  org.apache.zookeeper.ClientCnxn  - Socket connection established to 47.101.216.12/47.101.216.12:2181, initiating session
1140 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x0a38d7a3-SendThread(47.101.206.249:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - An exception was thrown while closing send thread for session 0x17a8fa62b6c9103 : Unable to read additional data from server sessionid 0x17a8fa62b6c9103, likely server has closed socket
1140 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - Session establishment request sent on 47.101.216.12/47.101.216.12:2181
1140 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x0a38d7a3] DEBUG org.apache.zookeeper.ClientCnxn  - Disconnecting client for session: 0x17a8fa62b6c9103
1141 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x0a38d7a3] INFO  org.apache.zookeeper.ZooKeeper  - Session: 0x17a8fa62b6c9103 closed
1147 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x0a38d7a3-EventThread] INFO  org.apache.zookeeper.ClientCnxn  - EventThread shut down for session: 0x17a8fa62b6c9103
1152 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] INFO  org.apache.zookeeper.ClientCnxn  - Session establishment complete on server 47.101.216.12/47.101.216.12:2181, sessionid = 0x27a8fa62b6c90fe, negotiated timeout = 40000
1159 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - Reading reply sessionid:0x27a8fa62b6c90fe, packet:: clientPath:/hbase/hbaseid serverPath:/hbase/hbaseid finished:false header:: 1,4  replyHeader:: 1,8590371658,0  request:: '/hbase/hbaseid,F  response:: #ffffffff000146d61737465723a3136303030ffffffa464ffffffac20fffffffeffffff89742c50425546a2434326661376532302d366138622d343365392d396636372d623663383633336238343538,s{8590270937,8590270937,1627199321070,1627199321070,0,0,0,0,67,0,8590270937}
1160 [main] DEBUG org.apache.hadoop.hbase.ipc.AbstractRpcClient  - Codec=org.apache.hadoop.hbase.codec.KeyValueCodec@26d9b808, compressor=null, tcpKeepAlive=true, tcpNoDelay=true, connectTO=10000, readTO=20000, writeTO=60000, minIdleTimeBeforeClose=120000, maxRetries=0, fallbackAllowed=false, bind address=null
1160 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Clean up env ===============
1160 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Delete table ===============
1200 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - Reading reply sessionid:0x27a8fa62b6c90fe, packet:: clientPath:/hbase/meta-region-server serverPath:/hbase/meta-region-server finished:false header:: 2,4  replyHeader:: 2,8590371658,0  request:: '/hbase/meta-region-server,F  response:: #ffffffff000146d61737465723a31363030305feffffffddffffffe2ffffffa422ffffffab1950425546a18ac6a696b656861646f6f70303510ffffff947d18fffffff3ffffff8cffffff8dffffffe5ffffffad2f100183,s{8590270976,8590349141,1627199326583,1627462136021,7,0,0,0,59,0,8590270976}
1370 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06-SendThread(47.101.216.12:2181)] DEBUG org.apache.zookeeper.ClientCnxn  - Reading reply sessionid:0x27a8fa62b6c90fe, packet:: clientPath:/hbase/master serverPath:/hbase/master finished:false header:: 3,4  replyHeader:: 3,8590371658,0  request:: '/hbase/master,F  response:: #ffffffff000146d61737465723a31363030304a5b31ffffffb5ffffffebffffff90ffffffecffffffd850425546a18ac6a696b656861646f6f70303210ffffff807d18ffffff87ffffffb8ffffff8cffffffe5ffffffad2f10018ffffff8a7d,s{8590270935,8590270935,1627199319870,1627199319870,0,0,0,178613079090425019,60,0,8590270935}
1374 [main] INFO  org.apache.hadoop.hbase.client.HBaseAdmin  - Started disable of lirui:student
2891 [main] INFO  org.apache.hadoop.hbase.client.HBaseAdmin  - Operation: DISABLE, Table Name: lirui:student, procId: 808 completed
3327 [main] INFO  org.apache.hadoop.hbase.client.HBaseAdmin  - Operation: DELETE, Table Name: lirui:student, procId: 810 completed
3328 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - Delete table success
3563 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Create up env  success
3563 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Create namespace ===============
3798 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Create namespace success
3798 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Create table ===============
5131 [main] INFO  org.apache.hadoop.hbase.client.HBaseAdmin  - Operation: CREATE, Table Name: lirui:student, procId: 813 completed
5131 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Create table success
5131 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Insert data ===============
5216 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Insert success
5216 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Put data cell version ===============
5226 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - Put data cell version success
5226 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Fetch data ===============
5231 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Fetch data success
5231 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - ===============
5231 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - -> keyvalues={lirui/info:class/1627536737413/Put/vlen=1/seqid=0, lirui/info:student_id/1627536737413/Put/vlen=14/seqid=0, lirui/name:/1627536737413/Put/vlen=5/seqid=0, lirui/score:programing/1627536737413/Put/vlen=2/seqid=0, lirui/score:understanding/1627536737413/Put/vlen=2/seqid=0}
5231 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - > Delete data ===============
5253 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  -  Delete data success
5253 [main] INFO  org.apache.hadoop.hbase.client.ConnectionImplementation  - Closing master protocol: MasterService
5254 [main] INFO  org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient  - Close zookeeper connection 0x740cae06 to 47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181
5254 [main] DEBUG org.apache.hadoop.hbase.ipc.AbstractRpcClient  - Stopping rpc client
5254 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06] DEBUG org.apache.zookeeper.ZooKeeper  - Closing session: 0x27a8fa62b6c90fe
5254 [ReadOnlyZKClient-47.101.216.12:2181,47.101.206.249:2181,47.101.204.23:2181@0x740cae06] DEBUG org.apache.zookeeper.ClientCnxn  - Closing client for session: 0x27a8fa62b6c90fe
5255 [main] DEBUG com.aibyte.bigdata.HBaseClientOperations  - -----> end
```