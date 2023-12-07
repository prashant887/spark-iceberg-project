package generic

import aws.SparkSessionUtils.{closeSparkSession, createSparkSession}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StructField, StructType}

import java.sql.Date
object DateSchema extends App {

  val spark:SparkSession=createSparkSession("dateTest",false)

  val schema: StructType = StructType(Array(
    StructField("vendor_id", LongType, true),
    StructField("update_ts", DateType, true)

  )
  )

  val data: Seq[Row] = Seq(
    Row(1: Long,Date.valueOf("2015-03-31"):Date)
  )

  val rdd = spark.sparkContext.parallelize(data)


  val df: DataFrame = spark.createDataFrame(rdd, schema)

  df.show()


}
