package conf

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

object ConfigReader {
  // ----------------------------------------------------------------------
  def getDataConf(filePathName: String): Map[String, List[String]] = {
    println("##【ConfigReader】《getDataConf》filePathName = " + filePathName)
    // ---------------------------------------
    var rs: Map[String, List[String]] = Map()
    // 【从外部读】---------------------------------------
    val is = new FileInputStream(new File(filePathName))
    // ---------------------------------------
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
      val info = v.split("#").toList
//      println(k + ":" + info)
      rs += (k -> info)
    }
    rs // 返回的结果
  }
  // ----------------------------------------------------------------------
  def getDFConf(filePathName: String): Map[String, Long] = {
    println("##【ConfigReader】《getDFConf》filePathName = " + filePathName)
    // ---------------------------------------
    var rs: Map[String, Long] = Map()
    // 【从外部读】---------------------------------------
    val is = new FileInputStream(new File(filePathName))
    // ---------------------------------------
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
//      println(k + ":" + v.toLong)
      rs += (k -> v.toLong)
    }
    rs // 返回的结果
  }
  // ----------------------------------------------------------------------
  def getSpaceConf(filePathName: String): Map[String, Int] = {
    println("##【ConfigReader】《getSpaceConf》filePathName = " + filePathName)
    // ---------------------------------------
    var rs: Map[String, Int] = Map()
    // 【从外部读】---------------------------------------
    val is = new FileInputStream(new File(filePathName))
    // ---------------------------------------
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
//      println(k + ":" + v.toLong)
      rs += (k -> v.toInt)
    }
    rs // 返回的结果
  }
  // ----------------------------------------------------------------------

}
