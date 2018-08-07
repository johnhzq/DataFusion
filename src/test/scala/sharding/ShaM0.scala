package sharding

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ShaM0 {
  def getRS(spark: SparkSession, df:DataFrame): DataFrame ={
    println("##【ShaM0】getRS")
    // ---------------------------------------
    import spark.implicits._ //隐式转换
    // ---------------------------------------
    val rs = df.groupByKey(r => r.getAs[String]("K") + "," + r.getAs[Int]("S"))
      .flatMapGroups((k, iter) => {
        // ----------------------------------------
        val rsBuffer = new ListBuffer[(Int, Double, Double, Long, Long, Int, String, List[String])]()
        // ----------------------------------------
        val K = k.split(",")(0)
        val S = k.split(",")(1).toInt
        // ----------------------------------------
        val tmpMap: Map[Long, String] = iter.map(r => (r.getAs[Long]("T"), r.getAs[String]("I"))).toMap
        val ts = tmpMap.keySet.toList.sortBy(+_)
        // ----------------------------------------
        var tmpMap2: Map[String, List[(Long,String)]] = Map()
        var t_head = ts.head
        var t_tail = ts.head
        val ab = new ArrayBuffer[(Long,String)]()
        for (t_p <- ts) {
          if (Math.abs(t_p - t_tail) < 600) {
            ab.append((t_p,tmpMap(t_p)))
            t_tail = t_p
          } else {
            tmpMap2 += ((t_head+"_"+t_tail) -> ab.toList)
            ab.clear()
            t_head = t_p
            ab.append((t_p,tmpMap(t_p)))
            t_tail = t_p
          }
        }
        tmpMap2 += ((t_head+"_"+t_tail) -> ab.toList)
        // ----------------------------------------
        for (t0_t1 <- tmpMap2.keySet) {
          val TIs = tmpMap2(t0_t1)
          val Is = TIs.map(TI => TI._2)
          // ----------------------------------------
          val times = TIs.map(TI => TI._1)
          val delay = calcDelay(times)
          val _mean = delay.sum.toDouble / delay.size
          val sqrt = delay.map(x => math.pow(x - _mean, 2))
          val _var = sqrt.sum / sqrt.size
//          println("##【*】" + K + ">>" + times + " mean=" + _mean + " var=" + _var)
          // ----------------------------------------
          val st = times.head
          val et = times.last
          val rs1 = (Is.length, _mean, _var, st, et, S, K, Is)
          // ----------------------------------------
          rsBuffer.append(rs1)
        }
        // ----------------------------------------
        rsBuffer.toList
      }).toDF("_n", "_mean", "_var", "st", "et", "S", "K", "Is")
    // ----------------------------------------
    rs
  }
  // ----------------------------------------------------------------------
  private def calcDelay(list: List[Long]): List[Long] = {
    list
      .sortBy(+_)
      .sliding(2)
      .map(x => x.last - x.head)
      .toList
  }
  // ----------------------------------------------------------------------


}
