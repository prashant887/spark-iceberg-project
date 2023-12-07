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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import SparkSessionUtils.{createSparkSession,closeSparkSession}
object GlueCatalogExample {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession=createSparkSession("IceBergeRead")
    spark.catalog.listCatalogs().show()
    spark.sql("show databases").show()
    spark.sql("USE artemis.artemis")
    spark.sql("show tables").show()

    val icebergTable = spark.read.format("iceberg").load("artemis.artemis.ctas_iceberg_parquet")

    icebergTable.show()

    icebergTable.groupBy(col("is_active")).count().show()

    closeSparkSession(spark)

  }

}
