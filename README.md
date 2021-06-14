# SparkDistCP

Spark dist cp is a tool to copy files from one hadoop to another like Distcp in Hive.

### how to run
you need to run like this
```shell script

spark-submit   --class com.squadron.SparkDistCP  --jars spark-distcp-assembly-[VERSION].jar spark-distcp_2.11-[VERSION].jar  --source hdfs:///user/duli/data --destination hdfs:///user/duli/data2 --numTasks 10
```
### parameters

```shell script
--numTasks <num>
# set tasks to run the tool
--ignoreErrors
# ignore error ouput
```


