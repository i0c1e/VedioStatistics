package com.victor.project.Utils

import org.apache.commons.lang3.time.FastDateFormat


object TimeUtils {
  val YYYDDMM_FORMAT = FastDateFormat.getInstance("YYYY-MM-DD hh:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("YYYYMMDD")

  def parseTime(timetext: String) = {
    val time = YYYDDMM_FORMAT.parse(timetext).getTime
    val target = TARGET_FORMAT.format(time)
    target
  }

  def main(args: Array[String]): Unit = {
    parseTime("2018-09-10 12:11:02")
  }

}
