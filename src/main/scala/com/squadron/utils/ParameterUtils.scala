package com.squadron.utils

object ParameterUtils {
  var handle_parameter_i:Int = 0
  var parameter:String = ""
  var numTasks:Int = 2
  var src:String = ""
  var dest:String= ""
  var ignoreErrors = false
  var threadWhileListFiles = 1
  def getNumTasks(): Int ={
    numTasks
  }
  def getSource():String = {
    src
  }
  def getDestination():String = {
    dest
  }
  def getIgnoreErrors():Boolean = {
    ignoreErrors
  }
  def getThreadWhileListFiles():Int = {
    threadWhileListFiles
  }

  def fun_get_next_parameter(parameter_array:Array[String]):Unit={
    LoggingUtils.log("Info","handle parameter before index " + handle_parameter_i)
    if ( handle_parameter_i <= 0  ){

      parameter=parameter_array(0);
      handle_parameter_i = 1;
    }else if(handle_parameter_i == parameter_array.length){
      parameter=null;
      handle_parameter_i = -1;
    }
    else{
      parameter=parameter_array(handle_parameter_i);
      handle_parameter_i = handle_parameter_i + 1;
    }
    LoggingUtils.log("Info","handle parameter after index " + handle_parameter_i)

  }

  def handle_parameters(parameter_array:Array[String]): Unit ={
    LoggingUtils.log("Info","handle parameters... ")
    fun_get_next_parameter(parameter_array)
    while (parameter != null) {
      LoggingUtils.log("Info","parameter "+ parameter)
      if (parameter == "--numTasks") {

        fun_get_next_parameter(parameter_array)
        numTasks = parameter.toInt;
        LoggingUtils.log("Info","get number of tasks " + numTasks)
      }else if(parameter == "--source"){
        fun_get_next_parameter(parameter_array)
        src = parameter
        LoggingUtils.log("Info","get source to copy: " + src)
      }else if (parameter == "--destination"){
        fun_get_next_parameter(parameter_array)
        dest = parameter
        LoggingUtils.log("Info","get destination to copy: " + dest)
      }else if (parameter == "--ignoreErrors"){
        ignoreErrors = true
        LoggingUtils.log("Info","get parameter ignoreErrors ")
      }else if (parameter == "--thread"){
        fun_get_next_parameter(parameter_array)
        threadWhileListFiles = parameter.toInt
        LoggingUtils.log("Info","get parameter thread while list files :"+threadWhileListFiles)

      }
      fun_get_next_parameter(parameter_array)
    }

  }
}
