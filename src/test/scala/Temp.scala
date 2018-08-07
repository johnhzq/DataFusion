import conf.Config
import org.apache.spark.sql.SparkSession
import reader.ReaderAdmin

object Temp {
  def main(args: Array[String]): Unit = {
//    println(Config().getCn2)
//    println(Config().getSpaceByGbno("44039603011400090007"))
//    val apple_mac: List[String] = List("c48466", "b0481a", "4c57ca")
//    println(apple_mac.indexOf("RunoobX"))
    // ----------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("DFusion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    val df = ReaderAdmin.getDataFrame("imsi", spark, "20180624")
    df.show()
//    df.foreach(r => println(r.toString()))
    df.printSchema()

  }
}
