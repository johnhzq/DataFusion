import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CommonUtil

object T_Config {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    //    val config = Config()
    //    println(Config().getCn2)
    // ---------------------------------------
    val spark = SparkSession.builder()
      //      .master("local")
      .appName("Config_T")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    A.getData(spark,"20180624").show()
    B.getData(spark,"20180624").show()
    B.getData(spark,"20180624").map(r => r.toString()).show()
    // ----------------------------------------
    spark.stop()
  }
}

object A {
  val dataName = "imsi"

  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    //    val config = spark.sparkContext.broadcast(Config()).value
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Ty_imsi_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    df.na.drop(check)
      .map(r => (
        //        if (r.getAs[Long](TS(0))==1529802076L) 1529802676L else r.getAs[Long](TS(0)),
        r.getAs[Long](TS(0)),
        config.getSpaceByGbno(r.getAs[String](TS(1))),
        r.getAs[String](CItems(0)),
        r.toString()
      ))
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // ----------------------------------------

    // ----------------------------------------
  }
}

object B {
  val dataName = "ajm"

  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    //    val config = spark.sparkContext.broadcast(Config()).value
    val config = Config()
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【B】dataPath = " + dataPath)
    println("##【B】check = " + check.toList.toString() )
    println("##【B】gbno = " + config.getSpaceByGbno("44039602011410020002"))
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
}
