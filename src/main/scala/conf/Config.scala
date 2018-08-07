package conf

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class Config private extends Serializable{
  // ----------------------------------------------------------------------
  private var th_window: Map[String, Long] = _
  private var dataType: Map[String, List[String]] = _
  private var dataInfo: Map[String, List[String]] = _
  private var gbno_space: Map[String, Int] = _
  private var station_space: Map[String, Int] = _
  // ----------------------------------------------------------------------
  def checkDataInfo(): Unit = {
    for (k1 <- dataType.keySet.toList.sorted) {
      val newitems = new ListBuffer[String]
      for (k2 <- dataType(k1)) {
        breakable {
          println("##【checkDataInfo】" + k1 + "->" + k2)
          val info = dataInfo(k2)
          if (info.size < 3) {
            println("##【X1】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          if (info(1).split("-").length != 2) {
            println("##【X2】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          if (info(2).split("-").length == 0) {
            println("##【X3】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          newitems.append(k2)
        }
        if (newitems.isEmpty) {
          dataType = dataType.-(k1)
        }
        else {
          dataType += (k1 -> newitems.toList)
        }
      }
    }
  }
  // ----------------------------------------------------------------------
  def getTh: Long = th_window("th")
  def getWindow: Long = th_window("window")
  def getThVsWindow: Double = th_window("th")*1.0/th_window("window")
  // ----------------------------------------------------------------------
  def getTypes(k: String): List[String] = dataType(k)
  def getBasePath(k: String): String = dataInfo(k).head
  def getTimeSpace(k: String): Array[String] = dataInfo(k)(1).split("-")
  def getCItems(k: String): Array[String] = dataInfo(k)(2).split("-")
  // ----------------------------------------------------------------------
  def getCn2: List[List[String]] = dataType.keySet.toList.sorted.combinations(2).toList
  // ----------------------------------------------------------------------
  def getSpaceByGbno(k: String): Int = {
    var rs = -1
    if(gbno_space.contains(k)){
      rs =  gbno_space(k)
    }
    rs
  }
  def getSpaceByStation(k: String): Int = {
    var rs = -1
    if(station_space.contains(k)){
      rs =  station_space(k)
    }
    rs
  }
  // ----------------------------------------------------------------------
  // ----------------------------------------------------------------------
}

object Config {
  var instant: Config = _
  def apply(): Config = {
    if (instant == null){
      instant = new Config
      // 【从外部读】---------------------------------------
      instant.th_window = ConfigReader.getDFConf("DataFusion/conf/DF.properties")
      instant.dataType = ConfigReader.getDataConf("DataFusion/conf/dataType.properties")
      instant.dataInfo = ConfigReader.getDataConf("DataFusion/conf/dataInfo.properties")
      instant.gbno_space = ConfigReader.getSpaceConf("DataFusion/conf/gbno_space.properties")
      instant.station_space = ConfigReader.getSpaceConf("DataFusion/conf/station_space.properties")
      // ---------------------------------------
      instant.checkDataInfo()
    }
    instant
  }
}
