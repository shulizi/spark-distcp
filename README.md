# SparkDistCP

Spark dist cp is a tool to copy files from one hadoop to another like Distcp in Hive.

### how to run
you need to run like this
```shell script

spark-submit   --class com.squadron.SparkDistCP  --jars spark-distcp-assembly-HEAD+20210613-2132.jar spark-distcp_2.11-HEAD+20210613-2132.jar  --source hdfs:///user/duli/data --destination hdfs:///user/duli/data2 --numTasks 10
```
### parameters

```shell script
-- numTasks <num>
# set tasks to run the tool
```


