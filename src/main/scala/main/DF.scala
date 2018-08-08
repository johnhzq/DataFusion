package main

import conf.Config
import fusion.FusM1
import org.apache.spark.sql.SparkSession
import reader.ReaderAdmin
import utils.CommonUtil

import scala.util.control.Breaks.{break, breakable}

object DF {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    if (args.length != 3) {
      System.err.println("【E】 Usage: DF <in> <out>")
      System.exit(2)
    }
    // ---------------------------------------
    val ST = args(0)
    val ET = args(1)
    if (!ST.matches("^[1|2]\\d{7}$") || !ET.matches("^[1|2]\\d{7}$")) {
      System.err.println("【E】 ST or ET 格式错误！！！")
    }
    val baseOutPath = args(2)
    // ---------------------------------------
    val spark = SparkSession.builder()
//      .master("local")
      .appName("DataFusion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val config = Config()
    // ----------------------------------------
    breakable {
      for (datePath <- CommonUtil.getDatePaths(ST, ET)) {
        println("## -----------------------------------")
        println("## *****         " + datePath + "        *****")
        println("## -----------------------------------")
        // ---------------------------------------
        breakable {
          for (c <- config.getCn2) {
            val typeI: String = c.head
            val typeII = c(1)
            breakable {
              for (dataNameI <- config.getTypes(typeI)) {
                breakable {
                  for (dataNameII <- config.getTypes(typeII)) {
                    val dataI = dataNameI
                    val dataII = dataNameII
                    val outPath = baseOutPath+"/"+typeI+"_"+typeII+"/"+dataI+"_"+dataII+"/"+datePath
                    println("## -----------------------------------")
                    println("## 【T】datePath = " + datePath)
                    println("## 【I】   dataI = " + typeI+"_"+dataI)
                    println("## 【I】  dataII = " + typeII+"_"+dataII)
                    println("## 【O】 outPath = " + outPath)
                    println("## -----------------------------------")
                    try {
                      // 【1】获取数据+预处理---------------------------------------
                      val df1 = ReaderAdmin.getDataFrame(dataI, spark, datePath)
                      val df2 = ReaderAdmin.getDataFrame(dataII, spark, datePath)
                      // 【2】融合---------------------------------------
                      val rs = FusM1.getRS(spark,df1,df2)
                      // 【3】保存结果---------------------------------------
//                      rs.show()
//                      rs.map(r=>r.getString(r.fieldIndex("I1"))+"|"+r.getString(r.fieldIndex("I2"))).show()
                      rs.write.save(outPath)
//                      rs.write.text(outPath)
                      // 【T】---------------------------------------
//                      val transactions = rs.select("K1","I1","K2","I2")
//                      ClaConfidence.getRS(spark,transactions)
                      // 【T】---------------------------------------
                    } catch {
                      case e: Exception => println("## 【E】" + e)
                    }
                    // ---------------------------------------
//                    break
                  }
                }
                // ---------------------------------------
//                break
              }
            }
            // ---------------------------------------
//            break
          }
        }
        // ---------------------------------------
        break
      }
    }
    spark.stop()
  }




}
