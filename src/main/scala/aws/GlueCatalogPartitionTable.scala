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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GlueCatalogPartitionTable {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession=createSparkSession("IceBergeTablePartition",true)

    val schema:StructType=StructType(Array(
      StructField("employee_id",IntegerType,nullable = true),
        StructField("first_name",StringType,nullable = true),
      StructField("last_name",StringType,nullable = true),
      StructField("email",StringType,nullable = true),
      StructField("phone_number",StringType,nullable = true),
      StructField("hire_date",DateType,nullable = true),
      StructField("job_id",StringType,nullable = true),
      StructField("salary",IntegerType,nullable = true),
      StructField("commission_pct",StringType,nullable = true),
      StructField("manager_id",IntegerType,nullable = true),
      StructField("department_id",IntegerType,nullable = true)
    ))

    val rawData=spark.
      read.
      schema(schema).
      option("header","true")
      .option("dateFormat","yy-MMM-d")
      .format("csv")
      .load("src/main/resources/employees.csv")

    rawData.show()

    val impDf=rawData.select(
      col("employee_id"),
      col("first_name"),
      col("email"),
      col("hire_date"),
      col("job_id"),
      col("department_id")
    )

    impDf.show()

    val table: String = "artemis.artemis.iceberg_employees_partition"

    val drop: Boolean = false

    if (drop) {
      val dropStmt: String = s"drop table if exists $table"

      spark.sql(dropStmt)

    }

    val exits: Boolean = spark.catalog.tableExists(table)

    println(s"Table $table Exits $exits")

    if (exits){
      impDf.writeTo(table)
        .option("mergeSchema","true")
        //.overwritePartitions()
        .append()

    }
    else {
      impDf.writeTo(table)
        .tableProperty("write.spark.accept-any-schema","true")
        .partitionedBy(col("job_id"))
        .createOrReplace()
    }

    closeSparkSession(spark)

  }

}
