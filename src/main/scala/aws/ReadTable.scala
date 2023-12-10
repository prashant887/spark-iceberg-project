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

import aws.SparkSessionUtils.createSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ReadTable {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession=createSparkSession("IceBergeTable",iceberg = true,wh = false)

    spark.sql("select current_schema() as schema").show()

    //spark.sql("SELECT * FROM artemis.ctas_iceberg_parquet;").show()

    println("List Catalog")
    spark.catalog.listCatalogs().show()

    println("Show databases")
    spark.sql("show databases").show()


    //spark.sql("USE artemis.artemis")
  //  spark.catalog.setCurrentDatabase("artemis") works in EMR

    spark.sql("show tables").show()

    val table:String="artemis.artemis.iceberg_spark_simple"


    val exits: Boolean = spark.catalog.tableExists(table)

    println(s"Table $table Exits $exits")




    if(exits) {
      println("Creating Table")
      spark.table(table).show()


    }
    else {
      println(s"${table} Table Not Found")
    }





  }

}
