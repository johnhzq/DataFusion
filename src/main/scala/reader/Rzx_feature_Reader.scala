package reader

import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import sharding.ShaM1

object Rzx_feature_Reader {
  val dataName = "rzx"
  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Rzx_feature_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // 【1】----------------------------------------
    val df1 = df.na.drop(check)
      .map(r => {
        val T = r.getAs[Long](TS(0))
        val S = config.getSpaceByGbno(r.getAs[String](TS(1)))
        val K = r.getAs[String](CItems(0)).replace("-", "").toLowerCase
        val I = r.toString()
        (T, S, K, I)
      })
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // 【2】---------------------------------------
    val df2 = ShaM1.getRS(spark, df1)
    val df2_0 = df2.select("_n", "_mean", "_var", "st", "et", "S", "K")
    val df2_1 = df2.select("st","S","K","Is").toDF("T", "S", "K", "I")
    df2_0.show()
    // ----------------------------------------

    // ----------------------------------------
    val rs = df2_1
    rs
  }
  // ----------------------------------------------------------------------
}
