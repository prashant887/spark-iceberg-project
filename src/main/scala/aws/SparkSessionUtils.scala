package aws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {

  def createSparkSession(name:String,iceberg:Boolean=false):SparkSession={
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName(name)

    if (iceberg){
      sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      sparkConf.set("spark.sql.catalog.artemis", "org.apache.iceberg.spark.SparkCatalog")
      sparkConf.set("spark.sql.catalog.artemis.warehouse", "s3://vmware-euc-cloud/data-dir/temp/transforms/")
      sparkConf.set("spark.sql.catalog.artemis.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      sparkConf.set("spark.sql.catalog.artemis.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    }

    //sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    //sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    spark

  }

  def closeSparkSession(spark:SparkSession):Unit={
    spark.stop()
    spark.close()
  }

}
