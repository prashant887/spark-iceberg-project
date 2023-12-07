package aws

/*
spark-submit \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://<bucket>/<prefix> \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager \
    --conf spark.sql.catalog.my_catalog.lock.table=myGlueLockTable
 */

import aws.SparkSessionUtils.{closeSparkSession, createSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TableNoDB {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession=createSparkSession("IceBergeTable",true)

    spark.sql("select current_schema() as schema").show()

    //spark.sql("SELECT * FROM artemis.ctas_iceberg_parquet;").show()

    println("List Catalog")
    spark.catalog.listCatalogs().show()

    println("Show databases")
    spark.sql("show databases").show()


    //spark.sql("USE artemis.artemis")
  //  spark.catalog.setCurrentDatabase("artemis") works in EMR

    spark.sql("show tables").show()

    val table:String="artemis.iceberg_simple"

    var data: Seq[Row] = Seq(
      Row(1: Long, 1000371: Long, 1.8f: Float, 15.32: Double, "N": String),
      Row(2: Long, 1000372: Long, 2.5f: Float, 22.15: Double, "N": String),
      Row(2: Long, 1000373: Long, 0.9f: Float, 9.01: Double, "N": String),
      Row(1: Long, 1000374: Long, 8.4f: Float, 42.13: Double, "Y": String)
    )

    var schema: StructType = StructType(Array(
      StructField("vendor_id", LongType, true),
      StructField("trip_id", LongType, true),
      StructField("trip_distance", FloatType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("store_and_fwd_flag", StringType, true)
    )
    )

    val rdd = spark.sparkContext.parallelize(data)


    val df: DataFrame = spark.createDataFrame(rdd, schema)

    df.show()

   df.writeTo(table).tableProperty("write.spark.accept-any-schema","true").createOrReplace()



  }

}
