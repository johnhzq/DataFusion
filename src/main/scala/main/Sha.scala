package main

import org.apache.spark.sql.SparkSession
import reader.ReaderAdmin
import sharding.ShaM1

object Sha {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("【E】 Usage: wordcount <in> <out>")
      System.exit(2)
    }
    // ---------------------------------------
    val dataName = args(0)
    val datePath = args(1)
    val baseOutPath = args(2)
    val outPath = baseOutPath+"/"+dataName+"/"+datePath
    // ---------------------------------------
    //
    // ---------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("Sha")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ---------------------------------------
    println("## -----------------------------------")
    println("## 【T】datePath = " + datePath)
    println("## 【I】    data = " + dataName)
    println("## 【O】 outPath = " + outPath)
    println("## -----------------------------------")
    // ---------------------------------------
    // 【1】获取数据+预处理---------------------------------------
    val df = ReaderAdmin.getDataFrame(dataName, spark, datePath)
//    println("##【Sha】df.count = " + df.count())
    // 【2】划分---------------------------------------
    val rs1 = ShaM1.getRS(spark,df)
    rs1.show()
//    println("##【Sha】rs1.count = " + rs1.count())
// ---------------------------------------
//    val part1 = rs1.select("_n","_mean")
//    part1.show()
//    part1.map(r => r.toString()).write.text(outPath+"/n_mean")
//    println("##【Sha】part1.count = " + part1.count())
    // 【3】保存结果---------------------------------------
//    rs1.map(r => r.toString()).write.text(outPath+"/M1")
    // ---------------------------------------
    spark.stop()
  }



}
