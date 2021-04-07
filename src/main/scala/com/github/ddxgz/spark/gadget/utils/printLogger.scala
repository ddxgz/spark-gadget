package com.github.ddxgz.spark.gadget.utils

import org.joda.time.{DateTime, DateTimeZone}

object LogLevel extends Enumeration {
  type LogLevel = Value
  val ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF = Value
}

/** PrintLogger is as the name incidated, just print log message via std print.
  *
  * Sometimes it fits well in the notebook development.
  */
class PrintLogger {
  import LogLevel._

  var name: String = "PrintLogger"
  var level: LogLevel = INFO

  def formatLog(msg: String, level: LogLevel): String = {
    val cur = DateTime.now(DateTimeZone.forID("CET"))
    s"[${this.name}][$cur][$level] $msg"
  }

  def printMsg(msg: String, level: LogLevel) = {
    if (this.level <= level) {
      println(formatLog(msg, level))
    }
  }

  def setLevel(level: LogLevel) = { this.level = level }

  def debug(msg: String) = { printMsg(msg, DEBUG) }

  def info(msg: String) = { printMsg(msg, INFO) }

  def warn(msg: String) = { printMsg(msg, WARN) }

  def error(msg: String) = { printMsg(msg, ERROR) }
}

object PrintLogger {
  import LogLevel._

  def apply(): PrintLogger = {
    var p = new PrintLogger
    p
  }

  def apply(name: String): PrintLogger = {
    var p = new PrintLogger
    p.name = name
    p
  }

  def apply(name: String, level: LogLevel): PrintLogger = {
    var p = new PrintLogger
    p.name = name
    p.level = level
    p
  }
}
