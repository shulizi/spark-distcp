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
    //    (hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data
    //      ,CopyDefinitionWithDependencies(
    //        SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //        ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data,List()))
    //    ,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //      ,CopyDefinitionWithDependencies(
    //        SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info.dat,14488255,File)
    //        ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //        ,List(
    //            SingleCopyDefinition(
    //                SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    //    ,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //      ,CopyDefinitionWithDependencies(
    //        SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info2.dat,14488255,File)
    //        ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //        ,List(
    //            SingleCopyDefinition(
    //                SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    LoggingUtils.log("Info","source rdd "+sourceRDD.map(x => x.toString).reduce((x,y) => x + "," + y))
    val destinationRDD = HandleFileUtils.getFilesFromDestinationHadoop(sparkSession.sparkContext, qualifiedDestinationPath)
    //(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data
    //   ,SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data,0,Directory))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2
    //   ,SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2,0,Directory))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //   ,SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat,14488255,File))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //   ,SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat,14488255,File))
    LoggingUtils.log("Info","destination rdd "+destinationRDD.map(x => x.toString).reduce((x,y) => x + "," + y))
    LoggingUtils.log("Info","SparkDistCP tasks number : \n" + sourceRDD.partitions.length)
    val joined = sourceRDD.fullOuterJoin(destinationRDD)
    //(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2
    //   ,(None,Some(SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2,0,Directory))))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data
    //   ,(Some(
    //       CopyDefinitionWithDependencies(
    //           SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //           ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data,List()))
    //     ,Some(
    //       SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data,0,Directory))))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //    ,(Some(
    //        CopyDefinitionWithDependencies(
    //            SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info.dat,14488255,File)
    //            ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat
    //            ,List(
    //                SingleCopyDefinition(
    //                    SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                    ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    //      ,Some(
    //          SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info.dat,14488255,File))))
    //,(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //      ,(Some(CopyDefinitionWithDependencies(
    //          SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data/ip_info2.dat,14488255,File)
    //          ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat
    //          ,List(
    //                SingleCopyDefinition(
    //                    SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
    //                    ,hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data))))
    //       ,Some(SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data2/data/ip_info2.dat,14488255,File))))
    LoggingUtils.log("Info","joined rdd "+joined.map(x => x.toString).reduce((x,y) => x + "," + y))
    val toCopy = joined.collect { case (_, (Some(s), _)) => s }


    val copyResult: RDD[String] = doCopy(toCopy)

    copyResult.foreach(_ => ())
    LoggingUtils.log("Info","Success to copy files")



  }


  private[squadron] def doCopy(sourceRDD: RDD[CopyDefinitionWithDependencies]): RDD[String] = {


    val serConfig = new ConfigSerializableDeser(sourceRDD.sparkContext.hadoopConfiguration)

    LoggingUtils.log("Info","do copy START ")
    var distcpresult = sourceRDD
      .mapPartitions {
        iterator =>
          val hadoopConfiguration = serConfig.get()
          val attemptID = TaskContext.get().taskAttemptId()
          val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

          iterator
            .flatMap(_.getAllCopyDefinitions)
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



}