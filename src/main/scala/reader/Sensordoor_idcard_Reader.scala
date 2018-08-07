package reader

import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CommonUtil

object Sensordoor_idcard_Reader {
  val dataName = "sensordoor"

  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Sensordoor_idcard_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    df.na.drop(check)
      .map(r => {
        val T = CommonUtil.getTime(r.getAs[String](TS(0)))
        val S = config.getSpaceByGbno(r.getAs[String](TS(1)))
        val K = r.getAs[String](CItems(0))
        val I = r.toString()
        (T, S, K, I)
      })
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // ----------------------------------------

    // ----------------------------------------
  }
  // ----------------------------------------------------------------------

  // ----------------------------------------------------------------------

}
