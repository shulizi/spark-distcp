package com.squadron

import java.net.URI

import com.squadron.objects._
import com.squadron.utils.{CopyUtils,LoggingUtils, HandleFileUtils, ParameterUtils, PathUtils}
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner, TaskContext}


object SparkDistCP {

  type KeyedCopyDefinition = (URI, CopyDefinitionWithDependencies)


  def main(args: Array[String]): Unit = {
    ParameterUtils.handle_parameters(args)
    val sparkSession = SparkSession.builder().getOrCreate()


    val src = new Path(ParameterUtils.getSource())
    var dest = new Path(ParameterUtils.getDestination())
    run(sparkSession, src, dest)

  }




  def run(sparkSession: SparkSession, sourcePath: Path, destinationPath: Path): Unit = {
    import sparkSession.implicits._




    val qualifiedSourcePath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, sourcePath)
    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, destinationPath)
    //sourceRDD.partitions.length = 2
    val sourceRDD = HandleFileUtils.getFilesFromSourceHadoop(sparkSession.sparkContext, qualifiedSourcePath.toUri, qualifiedDestinationPath.toUri,ParameterUtils.getNumTasks())
    val destinationRDD = HandleFileUtils.getFilesFromDestinationHadoop(sparkSession.sparkContext, qualifiedDestinationPath)

    LoggingUtils.log("Info","SparkDistCP tasks number : \n" + sourceRDD.partitions.length)
    val joined = sourceRDD.fullOuterJoin(destinationRDD)

    val toCopy = joined.collect { case (_, (Some(s), _)) => s }


    val copyResult: RDD[String] = doCopy(toCopy)

    copyResult.foreach(_ => ())
    LoggingUtils.log("Info","Success to copy files")



  }


  private[squadron] def doCopy(sourceRDD: RDD[CopyDefinitionWithDependencies]): RDD[String] = {


    val serConfig = new ConfigSerializableDeser(sourceRDD.sparkContext.hadoopConfiguration)
    //    batchAndPartitionFiles(sourceRDD):
    //    ((9,0),
    //      CopyDefinitionWithDependencies(
    //          SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info.dat,14488255,File)
    //          ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //          ,List(
    //             SingleCopyDefinition(
    //                SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    //  ,((9,0),
    //       CopyDefinitionWithDependencies(
    //           SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info2.dat,14488255,File)
    //           ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //           ,List(
    //               SingleCopyDefinition(
    //                  SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                  ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    // ,((7,0),
    //      CopyDefinitionWithDependencies(
    //           SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //          ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data
    //          ,List())
    LoggingUtils.log("Info","do copy START ")
    var distcpresult = batchAndPartitionFiles(sourceRDD)
      .mapPartitions {
        iterator =>
          val hadoopConfiguration = serConfig.get()
          val attemptID = TaskContext.get().taskAttemptId()
          val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

          iterator
            .flatMap(_._2.getAllCopyDefinitions)
            .collectMapWithEmptyCollection((d, z) => z.contains(d),
              d => {
                val r = CopyUtils.handleCopy(fsCache.getOrCreate(d.source.uri), fsCache.getOrCreate(d.destination), d, attemptID)
                r
              }
            )
      }
    LoggingUtils.log("Info","do copy END ")
    distcpresult
  }

  private[squadron] implicit class DistCPIteratorImplicit[B](iterator: Iterator[B]) {

    def collectMapWithEmptyCollection(skip: (B, Set[B]) => Boolean, action: B => String): Iterator[String] = {

      iterator.scanLeft((Set.empty[B], None: Option[String])) {
        case ((z, _), d) if skip(d, z) => (z, None)
        case ((z, _), d) =>
          (z + d, Some(action(d)))
      }
        .collect { case (_, Some(r)) => r }

    }

  }


  private[squadron] def batchAndPartitionFiles(rdd: RDD[CopyDefinitionWithDependencies]): RDD[((Int, Int), CopyDefinitionWithDependencies)] = {
    //Spark RDD分区是并行计算的一个计算单元，RDD在逻辑上被分为多个分区，分区的格式决定了并行计算的粒度，任务的个数是是由最后一个RDD的的分区数决定的
    //spark shuffle分为shuffle write和shuffle read阶段，一个partition对应一个reducer
    // rdd.partitions.length == 2
    val partitioner = rdd.partitioner.getOrElse(new HashPartitioner(rdd.partitions.length))
    //将  /user/duli/data下面的所有文件：ip_info2.dat、ip_info.dat拷贝到/user/duli/data2目录下
//    CopyDefinitionWithDependencies(
//      SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory),
//      hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data,List())
//
//   ,CopyDefinitionWithDependencies(
//     SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info.dat,14488255,File),
//     hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat,
//     List(
//       SingleCopyDefinition(
//         SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory),
//         hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data)))
//   ,CopyDefinitionWithDependencies(
//     SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info2.dat,14488255,File),
//     hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat,
//     List(
//       SingleCopyDefinition(
//         SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory),
//         hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data)))
    //repartitionAndSortWithinPartitions算子可以通过指定的分区器进行分组，并在分组内排序
    val sorted = rdd.map(v => (v.source.uri.toString, v)).repartitionAndSortWithinPartitions(partitioner).map(_._2)
    LoggingUtils.log("Info","sorted copy rdd :"+sorted.map(x => x.toString).reduce((x,y) => x + "," + y))
    val batched = sorted.mapPartitionsWithIndex(generateBatchedFileKeys()) //sorted
    var partitions = batched.partitionBy(CopyPartitioner(batched))
    LoggingUtils.log("Info","partitions rdd "+partitions.map(x => x.toString).reduce((x,y) => x + "," + y))
    partitions

  }

  private[squadron] def generateBatchedFileKeys(): (Int, Iterator[CopyDefinitionWithDependencies]) => Iterator[((Int, Int), CopyDefinitionWithDependencies)] = {
    (partition, iterator) =>
      iterator.scanLeft[(Int, Int, Long, CopyDefinitionWithDependencies)](0, 0, 0, null) {
        case ((index, count, bytes, _), definition) =>
          val newCount = count + 1
          val newBytes = bytes + definition.source.getLen
          if (newCount > 1000 || newBytes > 1073741824L) {
            (index + 1, 1, definition.source.getLen, definition)
          }
          else {
            (index, newCount, newBytes, definition)
          }
      }
        .drop(1)
        .map { case (index, _, _, file) => ((partition, index), file) }
  }

}