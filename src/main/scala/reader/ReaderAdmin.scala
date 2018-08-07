package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReaderAdmin {

  def getDataFrame(readerName: String, spark: SparkSession, datePath: String): DataFrame ={
    readerName match {
      case "ajm" => Ajm_idcard_Reader.getData(spark, datePath)
      case "sensordoor" => Sensordoor_idcard_Reader.getData(spark, datePath)
      case "szt" => Szt_subway_Reader.getData(spark, datePath)
      case "imsi" => Ty_imsi_Reader.getData(spark, datePath)
      case "rzx" => Rzx_feature_Reader.getData(spark, datePath)
      case "ap" => Ap_raw_Reader.getData(spark, datePath)
    }
  }
}
