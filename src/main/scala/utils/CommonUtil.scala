package utils

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer


object CommonUtil {
  // ----------------------------------------------------------------------
  def getDatePaths(ST: String, ET: String): List[String] ={
    val datePaths = new ListBuffer[String]
    val sdf = new SimpleDateFormat("yyyyMMdd")
    var date = sdf.parse(ST)
    datePaths.append(sdf.format(date))
    val dateET = sdf.parse(ET)
    val calendar = Calendar.getInstance()
    while (date.getTime < dateET.getTime) {
      calendar.setTime(date)
      calendar.add(Calendar.DAY_OF_MONTH, 1)
      date = calendar.getTime
      datePaths.append(sdf.format(date))
    }
    datePaths.toList
  }
  // ----------------------------------------------------------------------
  // 时间转换：str->long
  def getTime(yMdHms: String)={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(yMdHms).getTime/1000
  }
  // ----------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    println(CommonUtil.getDatePaths("20180624","20180626"))
    println(CommonUtil.getTime("2018-06-24 11:54:36")) // 1529812476
  }

}
