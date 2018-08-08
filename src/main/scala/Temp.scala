import org.apache.spark.sql.SparkSession
import reader.ReaderAdmin

object Temp {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    if (args.length != 1) {
      System.err.println("【E】 Usage: Temp <in> <out>")
      System.exit(2)
    }
    // ---------------------------------------
    val readerName = args(0)
    val spark = SparkSession.builder()
      .master("local")
      .appName("DFusion_count")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") //隐式转换
    val df = ReaderAdmin.getDataFrame(readerName, spark, "20180624")
    df.show(false)
    println(readerName+"="+df.count())
//    df.foreach(r => println(r.toString()))
//    df.printSchema()

  }
}
