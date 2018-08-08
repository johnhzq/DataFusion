package reader

import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CommonUtil

object Szt_subway_Reader {
  val dataName = "szt"
  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Szt_subway_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.format("csv")
      .option("sep", ",")
      //.option("inferSchema", "true") // 自动推测数据类型
      .load(dataPath)
    // 【1】----------------------------------------
    val df1 = df.na.drop(check)
      .filter(r => r.getAs[String]("_c3").matches(".*入站"))
      .map(r => {
        val T = CommonUtil.getTime(r.getAs[String](TS(0)).replace("T", " ").replace(".000Z", ""))
        val S = config.getSpaceByStation(r.getAs[String](TS(1)))
        val K = r.getAs[String](CItems(0))
        val I = r.toString()
        (T, S, K, I)
      })
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // ----------------------------------------

    // ----------------------------------------
    val rs = df1
    rs
  }
  // ----------------------------------------------------------------------
}
