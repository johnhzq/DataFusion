package fusion

import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object FusM1 {

  def getRS(spark: SparkSession, df1:DataFrame, df2:DataFrame): DataFrame ={
    println("##【FusM1】getRS")
    // ---------------------------------------
    import spark.implicits._ //隐式转换
    val config = Config()
    // ---------------------------------------
    val df10 = df1.flatMap(r => {
      val t = r.getLong(0)
      val t1 = t/config.getWindow // 整数部分
      var t2 = 0L
      val x = t*1.0/config.getWindow - t1 // 小数部分
      if(x<config.getThVsWindow){
        t2 = (t1-1)*(-10)-0 // L
      }
      else if(x>(1.0-config.getThVsWindow)){
        t2 = (t1+1)*(-10)-1 // R
      }
      t2 match {
        case 0 => List(
          (t1,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3))
        )
        case _ => List(
          (t1,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3)),
          (t2,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3))
        )
      }
    }).toDF("_T","T1","S","K1","I1")
    // ---------------------------------------
    val df20 = df2.flatMap(r => {
      val t = r.getLong(0)
      val t1 = t/config.getWindow
      var t2 = 0L
      val x = t*1.0/config.getWindow - t1
      if(x<config.getThVsWindow){
        t2 = t1*(-10)-1 // L'
      }
      else if(x>(1.0-config.getThVsWindow)){
        t2 = t1*(-10)-0 // R'
      }
      t2 match {
        case 0 => List(
          (t1,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3))
        )
        case _ => List(
          (t1,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3)),
          (t2,r.getLong(0),r.getInt(1),r.getString(2),r.getString(3))
        )
      }
    }).toDF("_T","T2","S","K2","I2")
    // ---------------------------------------
    val df10_1 = df10.filter(r=>r.getLong(0)>0)
    val df10_L = df10.filter(r=>r.getLong(0)<0 && r.getLong(0)%10==0)
    val df10_R = df10.filter(r=>r.getLong(0)<0 && r.getLong(0)%10!=0)
    val df20_1 = df20.filter(r=>r.getLong(0)>0)
    val df20_L = df20.filter(r=>r.getLong(0)<0 && r.getLong(0)%10==0)
    val df20_R = df20.filter(r=>r.getLong(0)<0 && r.getLong(0)%10!=0)
    // ---------------------------------------
    val rs1_0 = df10_1.join(df20_1,Seq("_T","S"))
    val rs1_L = df10_L.join(df20_L,Seq("_T","S"))
    val rs1_R = df10_R.join(df20_R,Seq("_T","S"))
    val rs1 = rs1_0.union(rs1_R).union(rs1_L)
//    rs1.show()
    // ---------------------------------------
    val rs2 = rs1.filter(r => {
      val t1 = r.getLong(r.fieldIndex("T1"))
      val t2 = r.getLong(r.fieldIndex("T2"))
      math.abs(t1-t2)<=config.getTh
    }).toDF()
    // ----------------------------------------
    rs2
  }
}
