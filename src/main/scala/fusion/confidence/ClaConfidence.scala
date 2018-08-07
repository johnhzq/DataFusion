package fusion.confidence

import conf.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.desc
import scala.collection.mutable.ListBuffer

object ClaConfidence {
  def getRS(spark: SparkSession, df: DataFrame): DataFrame = {
    println("##【ClaConfidence】getRS")
    // ---------------------------------------
    import spark.implicits._ //隐式转换
    val config = Config()
    // ---------------------------------------
    println("【transactions】----------------------------------------------------------------------")
    val transactions = df
    println("【patterns】----------------------------------------------------------------------")
    val patterns = transactions.flatMap(r => {
      val emptyString: String = null
      var A = (emptyString, emptyString)
      var B = (emptyString, emptyString)
      A = (r.getAs[String]("K1"), r.getAs[String]("I1"))
      B = (r.getAs[String]("K2"), r.getAs[String]("I2"))
      val items = List(A, B)
      (1 to items.size) flatMap items.combinations map (xs => xs.map(_.toString))
    })
      .map((_, 1))
      .toDF("K", "V")
    //    patterns.foreach(r => println(r.toString()))
    println("【combined】----------------------------------------------------------------------")
    val combined = patterns.groupBy(patterns.columns(0))
      .sum()
      //      .filter(r => r.getAs[Long](1)>1)
      .toDF("K", "V")
    //    combined.sort(desc("V")).foreach(r => println(r.toString()))
    println("【subpatterns】----------------------------------------------------------------------")
    val subpatterns = combined.flatMap(r => {
      val lb_rs = new ListBuffer[(List[String], (List[String], Long))]()
      val list = r.getAs[Seq[String]]("K").toList
      val frequency = r.getAs[Long]("V")
      lb_rs += ((list, (Nil, frequency)))
      val sublist = for {
        i <- list.indices
        xs = list.take(i) ++ list.drop(i + 1)
        if xs.nonEmpty
      } yield (xs, (list, frequency))
      lb_rs ++= sublist
      lb_rs.toList
    }).toDF("K", "V")
    //    subpatterns.foreach(r => println(r.toString()))
    println("【rules】----------------------------------------------------------------------")
    val rules = subpatterns.groupByKey(r => r.getAs[Seq[String]]("K"))
      .mapGroups((k, iter) => {
        val K = k.toList
        val lb_v = new ListBuffer[(List[String], Long)]()
        while (iter.hasNext) {
          val r = iter.next().getStruct(1)
          lb_v.append((r.getAs[Seq[String]](0).toList, r.getLong(1)))
        }
        val V = lb_v.toList
        (K, V)
      })
      .toDF("K", "V")
    //    rules.foreach(r => println(r.toString()))
    println("【assocRules】----------------------------------------------------------------------")
    val assocRules = rules.map(r => {
      val _1 = r.getAs[Seq[String]](0).toList
      val lb_v = new ListBuffer[(List[String], Long)]()
      val iter = r.getAs[Seq[Row]](1).iterator
      while (iter.hasNext) {
        val r = iter.next()
        lb_v.append((r.getAs[Seq[String]](0).toList, r.getLong(1)))
      }
      val _2 = lb_v.toList
      val fromCount = _2.find(p => p._1 == Nil).get
      val toList = _2.filter(p => p._1 != Nil)
      if (toList.isEmpty) Nil
      else {
        val result =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff _1
          } yield (_1, difference, confidence)
        result
      }
    })
    //    assocRules.foreach(r => println(r.toString()))
    println("【rs】----------------------------------------------------------------------")
    val temp = assocRules.flatMap(r => {
      val lb_rs = new ListBuffer[(List[String], Double)]()
      for (a <- r) {
        val K0 = a._1 ++ a._2
        val K1 = K0.sorted
        if (K0.head == K1.head) {
          lb_rs.append((K0, a._3))
        } else {
          lb_rs.append((K1, -a._3))
        }
      }
      lb_rs.toList
    })
      .toDF("K", "V")
    val rs = temp.groupByKey(r => r.getAs[Seq[String]]("K"))
      .mapGroups((k, iter) => {
        val _1 = k.toList
        val _2 = new Array[Double](2)
        while (iter.hasNext) {
          val confidence = iter.next().getDouble(1)
          if (confidence > 0) {
            _2(0) = confidence
          } else {
            _2(1) = -confidence
          }
        }
        (_1, _2.toList)
      })
      .toDF("K", "V")
    rs.foreach(r => println(r.toString()))
    // ----------------------------------------
    rs
  }
}
