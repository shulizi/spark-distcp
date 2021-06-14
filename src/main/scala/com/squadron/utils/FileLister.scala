package com.squadron.utils

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.squadron.objects.SerializableFileStatus
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import scala.collection.JavaConverters._
import scala.util.control.Breaks

class FileLister(fileSystem: FileSystem,path: Path) extends Runnable {
  private implicit class ScalaRemoteIterator[T](underlying: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = underlying.hasNext

    override def next(): T = underlying.next()
  }


  private val localfileSystem = FileSystem.get(fileSystem.getUri, fileSystem.getConf)
  //获得根结点
  //SerializableFileStatus(hdfs://hadoop02.ebay.fudan.edu:8020/user/duli/data,0,Directory)
  val pathRoot =  Some(SerializableFileStatus(fileSystem.getFileStatus(path)))
  LoggingUtils.log("Info","get file list pathroot: "+pathRoot.toString)
  val threadsWorking = new ConcurrentHashMap[UUID, Boolean]()
  val exceptions = new java.util.concurrent.ConcurrentLinkedQueue[Exception]()
  val fileList = new java.util.concurrent.LinkedBlockingQueue[(SerializableFileStatus, Seq[SerializableFileStatus])](pathRoot.map((_, Seq.empty)).toSeq.asJava)
  val directoryList = new java.util.concurrent.LinkedBlockingDeque[(Path, Seq[SerializableFileStatus])](List((path, pathRoot.toSeq)).asJava)


  private val uuid = UUID.randomUUID()
  threadsWorking.put(uuid, true)

  def getProcessed(): java.util.concurrent.LinkedBlockingQueue[(SerializableFileStatus, Seq[SerializableFileStatus])] ={
    fileList
  }
  def getToProcess(): java.util.concurrent.LinkedBlockingDeque[(Path, Seq[SerializableFileStatus])] ={
    directoryList
  }

  override def run(): Unit = {
    val loop = new Breaks;
    loop.breakable{
      while (true) {
        // while遍历 directoryList ,如果为目录，则追加进directoryList，如果为文件则追加进fileList 最后返回获取的fileList
        val path = directoryList.pollFirst()
        if (path == null) {
          loop.break
        }
        try {
          for (local <- fileSystem.listLocatedStatus(path._1) if !local.isSymlink) {
            if (local.isDirectory) {
              val s = SerializableFileStatus(local)
              directoryList.addFirst((local.getPath, path._2 :+ s))
              fileList.add((s, path._2))
            }
            if (local.isFile) {
              fileList.add((SerializableFileStatus(local), path._2))
            }
          }
        } catch {
          case e: Exception => {}
        }
      }
    }
  }
}
