package main

import org.apache.spark.sql.SparkSession
import reader.ReaderAdmin
import sharding.ShaM0

object Sha0 {
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
      .appName("Sha0")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") //隐式转换
    // ---------------------------------------
    println("## -----------------------------------")
    println("##【T】datePath = " + datePath)
    println("##【I】    data = " + dataName)
    println("##【O】 outPath = " + outPath)
    println("## -----------------------------------")
    // ---------------------------------------
    // 【1】获取数据+预处理---------------------------------------
    val df = ReaderAdmin.getDataFrame(dataName, spark, datePath)
//    println("##【Sha0】df.count = " + df.count())
    // 【2】划分---------------------------------------
    val rs0 = ShaM0.getRS(spark,df)
    rs0.show()
//    println("##【Sha0】rs0.count = " + rs0.count())
    // 【3】保存结果---------------------------------------
//    rs0.map(r => r.toString()).write.text(outPath+"/M0")
    // ---------------------------------------
    spark.stop()
  }



}
