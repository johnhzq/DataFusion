package reader

import conf.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Ap_raw_Reader {
  val dataName = "ap"

  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Ap_raw_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    val df1 = df.flatMap(r => {
      val rsBuffer = new ListBuffer[(Long, String, String, String, Long, String, Int, Int)]()
      val _c0 = r.getAs[Long](0)
      val _c1 = r.getAs[String](1)
      val _c2 = r.getAs[String](2)
      val _c3 = r.getAs[String](3)
      val MACs = r.getAs[Seq[Row]](4)
      val it = MACs.iterator
      while (it.hasNext) {
        val one = it.next()
        val _c4 = one.getAs[Long](0)
        val _c5 = one.getAs[String](1)
        val _c6 = one.getAs[Int](2)
        val _c7 = one.getAs[Int](3)
        rsBuffer.append((_c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7))
      }
      rsBuffer.toList
    }).toDF("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7")
    df1.na.drop(check)
      .map(r => {
        val T = r.getAs[Long](TS(0))
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
