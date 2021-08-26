# DistCP - Scala

## How do I run SparkDistCP?

You can run SparkDistCP from the command-line using:
```shell
bin/spark-submit --packages com.aibyte.bigdata.spark.examples --class com.aibyte.bigdata.spark.examples.SparkDistCP  [options] [source_path...] <target_path>
```

### Options:

| SparkDistCP Flag               |  Description                                            |
|--------------------------------|---------------------------------------------------------|
| `--i`                          | Ignore failures                                         |
| `--m <int>`                    |  Number of threads to use for building file listing     |

### 