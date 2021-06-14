package com.squadron.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Date

object LoggingUtils {
  def log(level: String, msg: String): String ={
    var time =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println("squadron "+ time + " "+level+" :"+msg)
    "squadron "+ time + " "+level+" :"+msg
  }
  def log(level: String, msg: String,throwable: String): String ={
    if (ParameterUtils.getIgnoreErrors())
      throw new RuntimeException(msg+throwable.toString)
    log(level,msg+throwable.toString)
  }
}
