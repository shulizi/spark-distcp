package com.squadron.utils

import java.io.FileNotFoundException
import java.net.URI

import com.squadron.objects._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import scala.util.{Failure, Success, Try}

object CopyUtils  {

  def handleCopy(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, taskAttemptID: Long): String = {

    val r = {
      if (definition.source.isDirectory) {
        LoggingUtils.log("Info","definition is directory "+definition)
        CopyUtils.createDirectory(destFS, definition)
      }
      else if (definition.source.isFile) {
        LoggingUtils.log("Info","definition is file "+definition)
        CopyUtils.copyFile(sourceFS, destFS, definition, taskAttemptID)
      }
      else
        throw new UnsupportedOperationException(s"Given file is neither file nor directory. Copy unsupported: ${definition.source.getPath}")
    }

    LoggingUtils.log("Info","handle copy "+r)
    r
  }


  def handleDelete(fs: FileSystem, uri: URI): String = {


    val path = new Path(uri)

    val r = deleteFile(fs, path)
    LoggingUtils.log("Info",r)
    r

  }


  private[utils] def deleteFile(fs: FileSystem, path: Path): String = {
    if (!fs.exists(path)) {
      var result:String = s"Path: [$path],delete path,file not exist"
      result
    }

    else {
      Try(fs.delete(path, true)) match {
        case Success(true) => s"Path: [$path],delete path,success"
        case Success(false) if !fs.exists(path) => s"Path: [$path],delete path,fail, file not exist"
        case Success(false) => throw new RuntimeException(s"Failed to delete directory [$path].")
        case Failure(e) => throw e
      }
    }
  }


  private[utils] def createDirectory(destFS: FileSystem, definition: SingleCopyDefinition): String = {
    val destPath = new Path(definition.destination)
    if (destFS.exists(destPath)) {
      var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,destination is exist"
      LoggingUtils.log("Info",result)
      result
    }
    else {
      val result = Try {
        if (destFS.exists(destPath.getParent)) {
          destFS.mkdirs(destPath)
          var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,make parent dir"
          result
        }
        else throw new FileNotFoundException(s"Parent folder [${destPath.getParent}] does not exist.")
      }.recover {
          case _: FileAlreadyExistsException =>
            var result: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,file is exist"
            result

        }
      result match {
        case Success(v) => v
        case Failure(e) =>
          throw e
      }
    }
  }


  private[utils] def copyFile(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, taskAttemptID: Long): String = {
    val destPath = new Path(definition.destination)
    LoggingUtils.log("Info","start to copy files")
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) =>
        var filecopy = performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = false, taskAttemptID)
        LoggingUtils.log("Info","fail to copy file " + filecopy)
        filecopy
      case Failure(e) =>
        throw e

      case Success(_)  =>
        performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = true, taskAttemptID)

      case Success(_) =>
        var filecopy: String = s"Source: [${definition.source.getPath.toUri}], Destination: [${definition.destination}] ,success to copy file"
        filecopy

    }
  }


  def performCopy(sourceFS: FileSystem, sourceFile: SerializableFileStatus, destFS: FileSystem, dest: URI, removeExisting: Boolean,taskAttemptID: Long): String = {

    val destPath = new Path(dest)

    val tempPath = new Path(destPath.getParent, s".sparkdistcp.$taskAttemptID.${destPath.getName}")

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourceFile.getPath))
        if (!destFS.exists(tempPath.getParent)) throw new RuntimeException(s"Destination folder [${tempPath.getParent}] does not exist")
        out = Some(destFS.create(tempPath, false))
        IOUtils.copyBytes(in.get, out.get, sourceFS.getConf.getInt("io.file.buffer.size", 4096))

      } catch {
        case e: Throwable => throw e
      } finally {
        in.foreach(_.close())
        out.foreach(_.close())
      }
    }.map {
      _ =>
        val tempFile = destFS.getFileStatus(tempPath)
        if (sourceFile.getLen != tempFile.getLen)
          throw new RuntimeException(s"Written file [${tempFile.getPath}] length [${tempFile.getLen}] did not match source file [${sourceFile.getPath}] length [${sourceFile.getLen}]")

        if (removeExisting) {
          val res = destFS.delete(destPath, false)
          if (!res) throw new RuntimeException(s"Failed to clean up existing file [$destPath]")
        }
        if (destFS.exists(destPath)) throw new RuntimeException(s"Cannot create file [$destPath] as it already exists")
        val res = destFS.rename(tempPath, destPath)
        if (!res) throw new RuntimeException(s"Failed to rename temporary file [$tempPath] to [$destPath]")
    } match {
      case Success(_) if removeExisting =>
        var result: String = s"Source: [${sourceFile.getPath.toUri}], Destination: [${dest}] ,perform copy,remove existing file"
        result
      case Success(_) =>
        var result: String = s"Source: [${sourceFile.getPath.toUri}], Destination: [${dest}] ,perform copy"
        result
      case Failure(e) =>
        throw e
    }

  }

  private[utils] def filesAreIdentical(f1: SerializableFileStatus, mc1: => Option[FileChecksum], f2: SerializableFileStatus, mc2: => Option[FileChecksum]): Boolean = {
    if (f1.getLen != f2.getLen) {
      LoggingUtils.log("Debug",s"Length [${f1.getLen}] of file [${f1.uri}] was not the same as length [${f2.getLen}] of file [${f2.uri}]. Files are not identical.")
      false
    }
    else {
      val c1 = mc1
      val c2 = mc2
      val same = mc1.flatMap(c1 => mc2.map(c1 ==)).getOrElse(true)
      if (same) {
        LoggingUtils.log("Debug",s"CRC [$c1] of file [${f1.uri}] was the same as CRC [$c2] of file [${f2.uri}]. Files are identical.")
        true
      }
      else {
        LoggingUtils.log("Debug",s"CRC [$c1] of file [${f1.uri}] was not the same as CRC [$c2] of file [${f2.uri}]. Files are not identical.")
        false
      }

    }

  }

}
