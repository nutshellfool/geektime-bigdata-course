# HBase Java API example

## Create namespace

* Shell command:
```shell
hbase(main):006:0> create_namespace 'lirui_test'
```
```shell
Took 0.2614 seconds
```

## Create table

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
## Insert Data

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

## Fetch Data

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

## Delete Data

* Shell command:
```shell
hbase(main):019:0> disable 'lirui_test:student'
Took 1.3026 seconds
```
```shell
hbase(main):020:0> drop 'lirui_test:student'
Took 0.5012 seconds
```