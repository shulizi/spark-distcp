package com.squadron.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Date

object LoggingUtils {
  def log(level: String, msg: String): Unit ={
    var time =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println("squadron "+ time + " "+level+" :"+msg)
  }
  def log(level: String, msg: String,throwable: Throwable): Unit ={
    log(level,msg+throwable.toString)
  }
}
