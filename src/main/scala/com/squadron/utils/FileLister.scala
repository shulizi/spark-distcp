package com.squadron.utils

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.squadron.objects.SerializableFileStatus
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.util.Try

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
  val processed = new java.util.concurrent.LinkedBlockingQueue[(SerializableFileStatus, Seq[SerializableFileStatus])](pathRoot.map((_, Seq.empty)).toSeq.asJava)
  val toProcess = new java.util.concurrent.LinkedBlockingDeque[(Path, Seq[SerializableFileStatus])](List((path, pathRoot.toSeq)).asJava)
  val exceptions = new java.util.concurrent.ConcurrentLinkedQueue[Exception]()


  private val uuid = UUID.randomUUID()
  threadsWorking.put(uuid, true)

  def getProcessed(): java.util.concurrent.LinkedBlockingQueue[(SerializableFileStatus, Seq[SerializableFileStatus])] ={
    processed
  }
  def getToProcess(): java.util.concurrent.LinkedBlockingDeque[(Path, Seq[SerializableFileStatus])] ={
    toProcess
  }

  override def run(): Unit = {
    while (threadsWorking.containsValue(true)) {
      Try(Option(toProcess.pollFirst(50, TimeUnit.MILLISECONDS))).toOption.flatten match {
        case None =>
          threadsWorking.put(uuid, false)
        case Some(p) =>
          LoggingUtils.log("Debug",s"Thread [$uuid] searching [${p._1}], waiting to process depth [${toProcess.size()}]")
          threadsWorking.put(uuid, true)
          try {
            localfileSystem
              .listLocatedStatus(p._1)
              .foreach {
                case l if l.isSymlink => throw new RuntimeException(s"Link [$l] is not supported")
                case d if d.isDirectory =>

                  val s = SerializableFileStatus(d)
                  toProcess.addFirst((d.getPath, p._2 :+ s))
                  processed.add((s, p._2))

                case f =>
                  processed.add((SerializableFileStatus(f), p._2))
              }
          } catch {
            case e: Exception => exceptions.add(e)
          }
      }
    }
  }
}
